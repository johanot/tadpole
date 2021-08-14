
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

use std::time::Duration;
use ttl_cache::TtlCache;
use std::sync::RwLock;
use crate::blobstore::UploadRange;
use crate::blobstore::UploadPart;

lazy_static! {
    static ref UPLOADS: RwLock<TtlCache<String, UploadData>> = RwLock::new(TtlCache::new(64));
}

const TEN_MEGS: usize = 8_388_608;

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

async fn gimme_raw(bucket: Bucket, path: String, mut writer: StreamWriter) {    
    bucket.get_object_stream(path, &mut writer).await.unwrap();
}

fn gimme(bucket: Bucket, info: BlobInfo, mut writer: StreamWriter) {
    tokio::spawn(async move {
        gimme_raw(bucket, info.digest.to_typefixed_string(), writer).await;
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
        let uuid = Uuid::new_v4().to_string();
        
        let res = self.bucket.initiate_multipart_upload(&format!("/{}", &uuid)).await?;
        let mut uploads = UPLOADS.write().map_err(|e| BlobError::Other{ inner: Box::new(e) })?;
        let data = UploadData::new(uuid.clone(), res.upload_id, res.key);
        uploads.insert(uuid.clone(), data.clone(), Duration::from_secs(300));

        Ok(data)
    }

    async fn patch(&self, upload_id: &UploadID, range: UploadRange, chunk: Bytes) -> Result<u64, BlobError> {
        let data = {
            let uploads = UPLOADS.read().map_err(|e| BlobError::Other{ inner: Box::new(e) })?;
            let data = uploads.get(upload_id).ok_or(BlobError::NotFound)?;
            //log::data("upload data on file", &data);
            //log::data("input range", &range);
            //log::data("data length", &chunk.len());
            data.clone()
        };

        //log::data("start s3 upload, range", &range);

        let part = self.bucket.put_multipart_chunk(chunk.to_vec(), &data.path, range.part_number, &data.backend_id).await?;
        {
            //log::info("end s3 upload");
            let mut uploads = UPLOADS.write().map_err(|e| BlobError::Other{ inner: Box::new(e) })?;
            let data_mut = uploads.get_mut(upload_id).ok_or(BlobError::NotFound)?;
            data_mut.range_offset = range.to;
            data_mut.parts.push(UploadPart(part))
        }
        Ok(range.to)
    }

    async fn get_upload_digest(&self, upload_id: &UploadID, input_digest: &Digest) -> Result<Digest, BlobError> {
        
        let (sender, receiver) = std::sync::mpsc::channel();
        let sw = StreamWriter::new_channeled(sender);
        gimme_raw(self.bucket.clone(), upload_id.to_string(), sw).await;

        Digest::from_channel(receiver).map_err(|e| BlobError::Other { inner: Box::new(e) })
    }

    async fn complete_uploaded_blob(
        &self,
        upload_id: &UploadID,
        input_digest: &Digest
    ) -> Result<(), BlobError> {

        let data = {
            let uploads = UPLOADS.read().map_err(|e| BlobError::Other{ inner: Box::new(e) })?;
            let data = uploads.get(upload_id).ok_or(BlobError::NotFound)?;
            log::data("upload data on file, when completing", &data);
            data.clone()
        };
        
        let mut parts = data.parts.clone();
        parts.sort();
        let parts = parts.iter().map(std::convert::Into::into).collect();

        self.bucket.complete_multipart_upload(&data.path, &data.backend_id, parts).await.map_err(|e| BlobError::Other{ inner: Box::new(e) })?;
        let mut uploads = UPLOADS.write().map_err(|e| BlobError::Other{ inner: Box::new(e) })?;
        uploads.remove(upload_id);
        Ok(())
    }

    async fn register_uploaded_blob(&self, upload_id: &UploadID, input_digest: &Digest) -> Result<(), BlobError> {
        self.bucket.copy_object_internal(upload_id, input_digest.to_typefixed_string()).await.unwrap();
        self.bucket.delete_object(upload_id).await.unwrap();
        Ok(())
    }
}