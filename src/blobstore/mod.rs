pub mod filesystem;
pub mod s3;

use async_trait::async_trait;
use std::error::Error;

use std::io::Read;
use std::convert::Into;

use crate::config::BlobStoreConfig;
use crate::types::ContentType;
use crate::types::Digest;
use std::path::PathBuf;
use std::marker::Send;
use std::io::Write;
use futures::stream::poll_fn;
use futures::task::Poll;
use dbc_rust_modules::log;
use std::io::BufRead;
use ::s3::serde_types::Part;


use warp::hyper::body::Bytes;
use warp::Buf;

type UploadID = String;

#[derive(Clone, Debug, Serialize)]
pub struct UploadData {
    pub upload_id: UploadID,
    pub backend_id: UploadID,
    pub path: String,
    pub range_offset: u64,
    pub parts: Vec<UploadPart>,
}

impl UploadData {
    pub fn new(upload_id: UploadID, backend_id: UploadID, path: String) -> Self {
        Self{
            upload_id,
            backend_id,
            path,
            range_offset: 0,
            parts: vec!(),
        }
    }
}

use core::cmp::Ordering;

#[derive(Debug, Clone, Serialize)]
pub struct UploadPart(Part);

impl std::convert::Into<Part> for &UploadPart {
    fn into(self) -> Part {
        self.0.clone()
    }
}

impl PartialOrd for UploadPart {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UploadPart {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.part_number.cmp(&other.0.part_number)
    }
}

impl PartialEq for UploadPart {
    fn eq(&self, other: &Self) -> bool {
        self.0.part_number == other.0.part_number
    }
}

impl Eq for UploadPart {
    /*fn eq(&self, other: &Self) -> bool {
        self.0.part_number == other.0.part_number
    }*/
}

#[derive(Clone, Debug, Serialize)]
pub struct UploadRange {
    pub from: u64,
    pub to: u64,
    pub part_number: u32,
}

impl UploadRange {
    pub fn new(from: u64, to: u64) -> Self {
        Self{
            from,
            to,
            part_number: 0,
        }
    }
    pub fn new_zero() -> Self {
        Self::new(0, 0)
    }
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn stat(&self, spec: BlobSpec) -> Result<BlobInfo, BlobError>;
    async fn get(&self, spec: BlobSpec, sw: StreamWriter) -> Result<BlobInfo, BlobError>;
    async fn get_upload_digest(&self, upload_id: &UploadID, input_digest: &Digest) -> Result<Digest, BlobError>;
    async fn start_upload(&self) -> Result<UploadData, BlobError>;
    async fn patch(&self, upload_id: &UploadID, range: Option<UploadRange>, input: Bytes) -> Result<u64, BlobError>;
    async fn complete_upload(
        &self,
        upload_id: &UploadID,
        input_digest: &Digest
    ) -> Result<(), BlobError> {

        let _total = self
            .complete_uploaded_blob(&upload_id, &input_digest)
            .await?;
    
        let store_digest = self.get_upload_digest(&upload_id, &input_digest).await?;
    
        log::info(&format!(
            "hash comparison: {:?}, {:?}",
            &store_digest, &input_digest
        ));
    
        if input_digest != &store_digest {
            return Err(BlobError::HashMismatch)
        }
    
        self.register_uploaded_blob(&upload_id, &input_digest).await
    }
    async fn complete_uploaded_blob(&self, upload_id: &UploadID, input_digest: &Digest) -> Result<(), BlobError>;
    async fn register_uploaded_blob(&self, upload_id: &UploadID, input_digest: &Digest) -> Result<(), BlobError>;
}

pub trait ToBlobStore<T> {
    fn to_blob_store(&self) -> T;
}

#[derive(Debug)]
pub struct BlobSpec {
    pub digest: Digest,
}

#[derive(Clone, Debug, Serialize)]
pub struct BlobInfo {
    pub content_type: ContentType,
    pub digest: Digest,
    pub size: u64,
}

use std::io::BufReader;
use warp::Stream;
type BlobStream = Box<(dyn Stream<Item = Result<Bytes, Box<(dyn Error + Send + Sync + 'static)>>> + Send + 'static)>;

pub struct Blob {
    pub info: BlobInfo,
    pub stream: BlobStream,
}

use std::pin::Pin;
use futures::stream::{self, StreamExt};

type StreamItemResult = Result<Bytes, Box<(dyn Error + Send + Sync + 'static)>>;
type StreamPollResult = Poll<Option<StreamItemResult>>;

use std::sync::Arc;

pub enum StreamWriter {
    Channeled(std::sync::mpsc::Sender<Bytes>),
    Streamed(hyper::body::Sender),
    Dummy,
}


impl StreamWriter {
    pub fn new(sender: hyper::body::Sender) -> Self {
        Self::Streamed(sender)
    }
    pub fn new_channeled(sender: std::sync::mpsc::Sender<Bytes>) -> Self {
        Self::Channeled(sender)
    }
    pub fn new_dummy() -> Self {
        Self::Dummy
    }
    pub fn write_sync(&mut self, bytes: Bytes) -> Result<(), hyper::body::Bytes> {
        match self {
            StreamWriter::Streamed(s) => s.try_send_data(bytes),
            StreamWriter::Channeled(s) => Ok(s.send(bytes).unwrap()), //TODO: don't panic
            StreamWriter::Dummy => Ok(()),
        }
    }
    pub async fn write_async(&mut self, bytes: Bytes) -> Result<(), hyper::Error> {
        match self {
            StreamWriter::Streamed(s) => s.send_data(bytes).await,
            StreamWriter::Channeled(s) => Ok(s.send(bytes).unwrap()), //TODO: don't panic
            StreamWriter::Dummy => Ok(()),
        }
    }
}

impl Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let len = buf.len();
        let mut bytes = Bytes::copy_from_slice(buf);
        use std::{thread, time};
        let ten_millis = time::Duration::from_micros(100);
        let mut attempts = 0;
        loop {
            match self.write_sync(bytes) {
                Ok(_) => { break; }
                Err(b) => { bytes = b;  }
            }
            attempts = attempts + 1;
            if attempts > 30000 {
                log::info("failed sending chunk");
                break;
            }
            thread::sleep(ten_millis);
        }
        //log::info("succeeded flushing");
        Ok(len)
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

use std::path::Path;
use std::fs::File;
pub fn file_to_writer(full_path: PathBuf, mut sw: StreamWriter) {
    tokio::spawn(async move {
        let f = File::open(&full_path).unwrap();
        let mut reader = BufReader::new(f);
        log::info("time to buffer");
        let mut chunks = 0;
        loop {
            chunks = chunks + 1;
            let buf_ = reader.fill_buf();
            match buf_ {
                Ok(buf) => {
                    let size = buf.len();
                    //
                    use std::{thread, time};
                    if size > 0 {
                        if match sw.write_async(Bytes::copy_from_slice(buf)).await {
                            Ok(_) => { reader.consume(size); Ok(false) },
                            Err(e) => Err(e),
                        }.unwrap() { break; }
                    } else {
                        break;
                    }
                },
                Err(e) => {
                    log::error("failed to read from buffer", &e);
                }
            }
        }
    });
}

pub fn empty_stream() -> BlobStream {
    let stream = poll_fn(move |_| -> StreamPollResult {
        Poll::Ready(None)
    });
    Box::new(stream)
}

impl std::convert::From<BlobInfo> for BlobSpec {
    fn from(info: BlobInfo) -> Self {
        BlobSpec{
            digest: info.digest.clone()
        }
    }
}

#[derive(Debug)]
pub enum BlobError {
    NotFound,
    HashMismatch,
    RangeUnacceptable { acceptable_range_offset: u64 },
    Other { inner: Box<dyn std::fmt::Debug> },
}
