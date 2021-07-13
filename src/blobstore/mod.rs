pub mod filesystem;
//pub mod s3;

use async_trait::async_trait;
use std::error::Error;

use std::io::Read;
use std::convert::Into;


#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct S3BlobStoreConfig {
    
}

pub struct S3BlobStore {
    
}


use crate::types::ContentType;
use crate::types::Digest;
use std::path::PathBuf;


use warp::hyper::body::Bytes;

type UploadID = String;

#[async_trait]
pub trait BlobStore<C> {
    fn init(config: C) -> Result<Self, BlobError>
    where
        Self: Sized;
    fn stat<S: Into<BlobSpec>>(&self, spec: S) -> Result<BlobInfo, BlobError>;
    fn get<S: Into<BlobSpec>>(&self, spec: S) -> Result<Blob, BlobError>;
    fn get_upload_digest(&self, upload_id: &UploadID) -> Result<Digest, BlobError>;
    fn start_upload(&self) -> Result<UploadID, BlobError>;
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
    pub path: PathBuf,
}

pub struct Blob {
    pub info: BlobInfo,
    pub body: Option<warp::hyper::Body>,
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
