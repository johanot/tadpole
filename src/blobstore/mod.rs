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


use warp::hyper::body::Bytes;

type UploadID = String;

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn stat(&self, spec: BlobSpec) -> Result<BlobInfo, BlobError>;
    async fn get(&self, spec: BlobSpec) -> Result<Blob, BlobError>;
    fn get_upload_digest(&self, upload_id: &UploadID) -> Result<Digest, BlobError>;
    async fn start_upload(&self) -> Result<UploadID, BlobError>;
    async fn patch(&self, upload_id: &UploadID, input: Bytes) -> Result<u64, BlobError>;
    async fn complete_upload(
        &self,
        upload_id: &UploadID,
        input_digest: &Digest
    ) -> Result<(), BlobError>;
}

pub trait ToBlobStore<T> {
    fn to_blob_store(&self) -> T;
}

#[derive(Debug)]
pub struct BlobSpec {
    pub digest: Digest,
}

#[derive(Debug)]
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

pub struct StreamWriter(VecDeque<StreamItemResult>);
use std::collections::VecDeque;

impl StreamWriter {
    fn new() -> Self {
        Self(VecDeque::new())
    }
}

impl Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let len = buf.len();
        self.0.push_back(Ok(Bytes::copy_from_slice(buf)));
        Ok(len)
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub fn writer_to_stream(mut writer: StreamWriter) -> BlobStream {
    let stream = poll_fn(move |_| -> StreamPollResult {
        Poll::Ready(writer.0.pop_front())
    });
    Box::new(stream)
}

pub fn reader_to_stream<R: 'static + Read + Send + Sync>(mut reader: BufReader<R>) -> BlobStream {
    let stream = poll_fn(move |_| -> StreamPollResult {
        match reader.fill_buf() {
            Ok(c) => {
                let size = c.len();
                if size > 0 {
                    let res = Poll::Ready(Some(Ok(Bytes::copy_from_slice(c))));
                    reader.consume(size);
                    res
                } else {
                    Poll::Ready(None)
                }
            },
            Err(e) => {
                log::error("failed to read from buffer", &e);
                Poll::Ready(None)
            }
        }
    });
    Box::new(stream)
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
    Other { inner: Box<dyn std::fmt::Debug> },
}
