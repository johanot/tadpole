pub mod filesystem;

use async_trait::async_trait;
use std::error::Error;
use std::io::BufReader;
use std::io::Read;
use uuid::Uuid;
use warp::{Buf, Stream};

use std::sync::Arc;

use crate::types::ContentType;
use crate::types::Digest;
use crate::MonolithicUpload;
use warp::hyper::body::Bytes;

type UploadID = String;

#[async_trait]
pub trait BlobStore<C> {
    fn init(config: C) -> Result<Self, BlobError>
    where
        Self: Sized;
    fn head(&self, spec: &BlobSpec) -> Result<BlobInfo, BlobError>;
    fn get(&self, spec: &BlobSpec) -> Result<Blob, BlobError>;
    fn start_upload(&self) -> Result<UploadID, BlobError>;
    async fn patch(&self, upload_id: &UploadID, input: Bytes) -> Result<u64, BlobError>;
    async fn complete_upload(
        &self,
        upload_id: &UploadID,
        digest: &Digest,
    ) -> Result<Digest, BlobError>;
}

pub trait ToBlobStore<T> {
    fn to_blob_store(&self) -> T;
}

#[derive(Debug)]
pub struct BlobSpec {
    pub repo: String,
    pub digest: Digest,
}

pub struct BlobInfo {
    pub content_type: ContentType,
    pub digest: Digest,
    pub size: u64,
}

pub struct Blob {
    pub info: BlobInfo,
    pub reader: Box<dyn Read>,
}

#[derive(Debug)]
pub enum BlobError {
    NotFound,
    HashMismatch,
    Other { inner: Box<dyn Error> },
}
