use crate::blobstore::{BlobError, BlobInfo, BlobSpec, BlobStore, ToBlobStore, UploadID};
use crate::log;
use async_trait::async_trait;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::BufReader;
use std::io::BufWriter;
use std::marker::Send;
use crate::blobstore::Blob;
use crate::blobstore::UploadData;

use std::path::PathBuf;
use uuid::Uuid;
use crate::blobstore::StreamWriter;


use crate::types::{ContentType, Digest, DigestAlgo};

use warp::hyper::body::Bytes;

use std::io::BufRead;
use std::io::Read;

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

impl FileSystemBlobStore {
    fn init(config: FileSystemBlobStoreConfig) -> Result<Self, BlobError> {
        fs::create_dir_all(&config.store_path)
            .map_err(|e| BlobError::Other { inner: Box::new(e) })?;
        Ok(Self { config })
    }
}

#[async_trait]
impl BlobStore for FileSystemBlobStore {
    async fn stat(&self, spec: BlobSpec) -> Result<BlobInfo, BlobError> {
        //let spec: BlobSpec = spec.into();
        
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

    async fn get(&self, spec: BlobSpec, writer: StreamWriter) -> Result<BlobInfo, BlobError> {
        let info = self.stat(spec).await?;

        let full_path = self
            .config
            .store_path
            .join(&info.digest.to_typefixed_string());

        //TODO: stream instead of buffering entire blob to memory
        //f.read_to_end(&mut buffer).map_err(|e| BlobError::Other { inner: Box::new(e) })?;
        use crate::blobstore::file_to_writer;
        file_to_writer(full_path.to_owned(), writer);
        log::info("returning");
        Ok(info)
    }

    async fn start_upload(&self) -> Result<UploadData, BlobError> {
        let uuid = Uuid::new_v4();
        let full_path = PathBuf::from("blobstore").join(&uuid.to_string());

        let len = {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full_path)
                .unwrap();

            file.metadata().unwrap().len()
        };

        log::info(&format!("initial length: {}", len));
        Ok(UploadData{
            upload_id: uuid.to_string(),
            path: full_path.to_str().unwrap_or("").to_owned(),
        })
    }

    async fn patch(&self, upload_id: &UploadID, input: Bytes) -> Result<u64, BlobError> {
        let full_path = self.config.store_path.join(&upload_id);
        let mut file = OpenOptions::new().append(true).open(&full_path).unwrap();

        let len = file.metadata().unwrap().len();
        if !input.is_empty() {
            let mut writer = BufWriter::with_capacity(4096, &mut file);
            let mut reader = BufReader::with_capacity(4096, &*input);

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


    fn get_upload_digest(&self, upload_id: &UploadID) -> Result<Digest, BlobError> {
        let full_path = self.config.store_path.join(&upload_id);
        let file = File::open(&full_path).unwrap();
        Digest::from_reader(BufReader::with_capacity(8 * 1024 * 1024, file)).map_err(|_| BlobError::HashMismatch) //TODO: map to different error
    }

    async fn complete_upload(
        &self,
        upload_id: &UploadID,
        input_digest: &Digest
    ) -> Result<(), BlobError> {
        let full_path = self.config.store_path.join(&upload_id);
        let final_path = self
            .config
            .store_path
            .join(&input_digest.to_typefixed_string());

        
        std::fs::rename(&full_path, &final_path).unwrap();
        Ok(())
    }
}
