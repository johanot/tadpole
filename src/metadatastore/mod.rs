pub mod etcd;
pub mod filesystem;

use async_trait::async_trait;
use std::error::Error;

use std::io::Read;
use crate::types::ContentType;
use crate::types::Digest;
use crate::blobstore::Blob;
use std::str::FromStr;
use std::convert::Infallible;

use crate::config::Repository;

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn write_spec(&self, tag: &ManifestSpec, manifest_digest: &Digest) -> Result<(), MetadataError>;
    async fn read_spec(&self, tag: &ManifestSpec) -> Result<Digest, MetadataError>;
}

pub trait ToMetadataStore<T> {
    fn to_metadata_store(&self) -> T;
}

#[derive(Debug)]
pub enum MetadataError {
    Ambiguous,
    NotFound,
    Other { inner: Box<dyn std::fmt::Debug> },
}

#[derive(Debug)]
pub struct ManifestSpec {
    pub repo: Repository,
    pub name: String,
    pub reference: ImageRef,
}

use crate::BlobInfo;

pub struct Manifest {
    pub info: BlobInfo,
    pub body: hyper::body::Body,
}

#[derive(Clone, Debug)]
pub enum ImageRef {
    Tag(String),
    Digest(Digest),
}

impl FromStr for ImageRef {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Digest::from_str(s)
            .map(|d| Self::Digest(d))
            .or(Ok(ImageRef::Tag(s.to_string())))
    }
}

impl ToString for ImageRef {
    fn to_string(&self) -> String {
        match self {
            ImageRef::Tag(t) => t.clone(),
            ImageRef::Digest(d) => d.to_typefixed_string(),
        }
    }
}