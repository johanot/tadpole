use crate::blobstore::{Blob, BlobError, BlobInfo, BlobSpec, BlobStore, ToBlobStore, UploadID};
use crate::log;
use async_trait::async_trait;
use sha2::Digest;
use sha2::Sha256;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;

use std::path::PathBuf;
use uuid::Uuid;


use crate::types::{ContentType, Digest as TadpoleDigest, DigestAlgo};



use std::io::Write;



use warp::hyper::body::Bytes;

use std::io::BufRead;

#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSystemBlobStoreConfig {
    store_path: PathBuf,
    //TODO: moar fields
}

pub struct FileSystemBlobStore {
    config: FileSystemBlobStoreConfig,
}

impl ToBlobStore<FileSystemBlobStore> for FileSystemBlobStoreConfig {
    fn to_blob_store(&self) -> FileSystemBlobStore {
        FileSystemBlobStore::init(self.clone()).unwrap()
    }
}

#[async_trait]
impl BlobStore<FileSystemBlobStoreConfig> for FileSystemBlobStore {
    fn init(config: FileSystemBlobStoreConfig) -> Result<Self, BlobError> {
        fs::create_dir_all(&config.store_path)
            .map_err(|e| BlobError::Other { inner: Box::new(e) })?;
        Ok(Self { config })
    }

    fn head(&self, spec: &BlobSpec) -> Result<BlobInfo, BlobError> {
        let full_path = self
            .config
            .store_path
            .join(&spec.digest.to_typefixed_string());
        match full_path.exists() {
            true => {
                let fs_meta = std::fs::metadata(&full_path)
                    .map_err(|e| BlobError::Other { inner: Box::new(e) })?;

                Ok(BlobInfo {
                    content_type: ContentType::OctetStream,
                    size: fs_meta.len(),
                    digest: spec.digest.clone(),
                })
            }
            false => Err(BlobError::NotFound),
        }
    }

    fn get(&self, _spec: &BlobSpec) -> Result<Blob, BlobError> {
        Err(BlobError::NotFound)
    }

    fn start_upload(&self) -> Result<UploadID, BlobError> {
        let uuid = Uuid::new_v4();
        let full_path = PathBuf::from("blobstore").join(&uuid.to_string());

        let len = {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(full_path)
                .unwrap();

            file.metadata().unwrap().len()
        };

        log::info(&format!("initial length: {}", len));
        Ok(uuid.to_string())
    }

    async fn patch(&self, upload_id: &UploadID, input: Bytes) -> Result<u64, BlobError> {
        let full_path = self.config.store_path.join(&upload_id);
        let mut file = OpenOptions::new().append(true).open(&full_path).unwrap();

        let len = file.metadata().unwrap().len();
        if !input.is_empty() {
            let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, &mut file);
            let mut reader = BufReader::with_capacity(8 * 1024 * 1024, &*input);

            let written = std::io::copy(&mut reader, &mut writer).unwrap();

            writer.flush().unwrap();
            let total = len + written;

            log::info(&format!("wrote: {} bytes", total));
            Ok(total)
        } else {
            log::info("No bytes to write");
            Ok(len)
        }
    }

    async fn complete_upload(
        &self,
        upload_id: &UploadID,
        input_digest: &TadpoleDigest,
    ) -> Result<TadpoleDigest, BlobError> {
        let full_path = self.config.store_path.join(&upload_id);
        let final_path = self
            .config
            .store_path
            .join(&input_digest.to_typefixed_string());

        let check_digest = {
            let file = File::open(&full_path).unwrap();
            let mut hasher = Sha256::new();
            let mut br = BufReader::with_capacity(8 * 1024 * 1024, file);
            loop {
                let buffer = br.fill_buf().unwrap();
                let buf_len = buffer.len();
                if buf_len > 0 {
                    hasher.update(buffer);
                    br.consume(buf_len);
                } else {
                    break;
                }
            }
            TadpoleDigest {
                algo: DigestAlgo::Sha256,
                value: hasher.finalize().to_vec(),
            }
        };

        log::info(&format!(
            "hash comparison: {:?}, {:?}",
            &check_digest, &input_digest
        ));

        if &check_digest == input_digest {
            std::fs::rename(&full_path, &final_path).unwrap();
            Ok(check_digest)
        } else {
            Err(BlobError::HashMismatch)
        }
    }
}