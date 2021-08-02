
use s3::{Bucket, Region};
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
use crate::blobstore::UploadData;
use s3::serde_types::InitiateMultipartUploadResponse;
use dbc_rust_modules::log;

use uuid::Uuid;
use futures::StreamExt;

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

impl std::convert::From<anyhow::Error> for BlobError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other{
            inner: Box::new(err)
        }
    }
}

impl std::convert::From<InitiateMultipartUploadResponse> for UploadData {
    fn from(response: InitiateMultipartUploadResponse) -> Self {
        Self{
            upload_id: response.upload_id,
            path: response.key,
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
        let bucket = Bucket::new(&config.bucket_name, config.region.clone(), config.credentials.clone())?;
        Ok(S3BlobStore{
            bucket
        })
    }
}

fn gimme(bucket: Bucket, info: BlobInfo, mut writer: StreamWriter) {
    tokio::spawn(async move {
        bucket.get_object_stream(&info.digest.to_typefixed_string(), &mut writer).await.unwrap();
    });
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn stat(&self, spec: BlobSpec) -> Result<BlobInfo, BlobError> {
        let spec: BlobSpec = spec.into();
        let res = self.bucket.head_object(&spec.digest.to_typefixed_string()).await;
        log::info(&format!("{:?}", &res));
        match res {
            Ok((_, 404)) => Err(BlobError::NotFound),
            Ok((obj, 200)) => Ok(BlobInfo{
                content_type: ContentType::OctetStream, //hardcoded for now
                digest: spec.digest.clone(),
                size: obj.content_length.unwrap() as u64,
            }),
            Ok((_, code)) => Err(BlobError::Other { inner: Box::new(format!("unknown http response code from s3: {}", code)) }),
            Err(e) => Err(BlobError::Other { inner: Box::new(e) }),
        }
    }

    async fn get(&self, spec: BlobSpec, mut writer: StreamWriter) -> Result<BlobInfo, BlobError> {
        let info = self.stat(spec).await?;    
        log::data("info", &format!("{:?}", &info));    
        
        gimme(self.bucket.clone(), info.clone(), writer);

        Ok(info)
    }

    async fn start_upload(&self) -> Result<UploadData, BlobError> {
        let uuid = Uuid::new_v4();
        Ok(self.bucket.initiate_multipart_upload(&uuid.to_string()).await.map(|r| r.into())?)
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