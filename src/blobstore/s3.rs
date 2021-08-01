
use s3::{Bucket, Region, S3Error};
use s3::creds::Credentials;
use async_trait::async_trait;
use crate::blobstore::{BlobError, BlobInfo, BlobSpec, BlobStore, ToBlobStore, UploadID};
use crate::types::{ContentType, Digest, DigestAlgo};
use hyper::body::Bytes;
use serde::de::{self, Deserialize, Deserializer};
use std::marker::Send;
use std::io::Write;
use crate::blobstore::Blob;
use crate::blobstore::StreamWriter;

#[derive(Deserialize, Debug)]
struct CredentialsHelper {
    access_key: String,
    secret_key: String, //TODO: take in files or references instead of the secret itself
}

fn deserialize_credentials<'de, D>(deserializer: D) -> Result<Credentials, D::Error>
where
    D: Deserializer<'de>,
{
    let helper: CredentialsHelper = Deserialize::deserialize(deserializer)?;
    Credentials::new(Some(&helper.access_key), Some(&helper.secret_key), None, None, None).map_err(de::Error::custom)
}

fn deserialize_region<'de, D>(deserializer: D) -> Result<Region, D::Error>
where
    D: Deserializer<'de>,
{
    use std::str::FromStr;
    let region: String = Deserialize::deserialize(deserializer)?;
    Region::from_str(&region).map_err(de::Error::custom)
}

#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct S3BlobStoreConfig {
    bucket_name: String,
    #[serde(deserialize_with = "deserialize_credentials")]
    credentials: Credentials,
    #[serde(deserialize_with = "deserialize_region")]
    region: Region,
}

pub struct S3BlobStore {
    bucket: Bucket,
}

impl std::convert::From<S3Error> for BlobError {
    fn from(err: S3Error) -> Self {
        Self::Other{
            inner: Box::new(err)
        }
    }
}

impl ToBlobStore<S3BlobStore> for S3BlobStoreConfig {
    fn to_blob_store(&self) -> S3BlobStore {
        S3BlobStore::init(self.clone()).unwrap()
    }
}

impl S3BlobStore {
    fn init(config: S3BlobStoreConfig) -> Result<Self, BlobError> {
        Ok(S3BlobStore{
            bucket: Bucket::new(&config.bucket_name, config.region.clone(), config.credentials.clone())?
        })
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn stat(&self, spec: BlobSpec) -> Result<BlobInfo, BlobError> {
        let spec: BlobSpec = spec.into();
        match self.bucket.head_object(&spec.digest.to_typefixed_string()).await {
            Ok((obj, _)) => Ok(BlobInfo{
                content_type: ContentType::OctetStream, //hardcoded for now
                digest: spec.digest.clone(),
                size: obj.content_length.unwrap() as u64,

            }),
            Ok((_, 404)) => Err(BlobError::NotFound),
            Err(e) => Err(BlobError::Other { inner: Box::new(e) }),
        }
    }

    async fn get(&self, spec: BlobSpec) -> Result<Blob, BlobError> {
        let info = self.stat(spec).await?;
        
        let mut writer = StreamWriter::new();
        let status_code = self.bucket.get_object_stream(&info.digest.to_typefixed_string(), &mut writer).await?;
        
        use crate::blobstore::writer_to_stream;
        Ok(Blob{
            info,
            stream: writer_to_stream(writer)
        })
    }

    async fn start_upload(&self) -> Result<UploadID, BlobError> {
        Err(BlobError::NotFound)
    }

    async fn patch(&self, upload_id: &UploadID, input: Bytes) -> Result<u64, BlobError> {
        Err(BlobError::NotFound)
    }

    fn get_upload_digest(&self, upload_id: &UploadID) -> Result<Digest, BlobError> {
        Err(BlobError::NotFound)
    }

    async fn complete_upload(
        &self,
        upload_id: &UploadID,
        input_digest: &Digest,
    ) -> Result<(), BlobError> {
        
        Err(BlobError::HashMismatch)
    }
}