use once_cell::sync::OnceCell;
use reqwest::Url;
use serde::de::{self, Deserialize, Deserializer};

use crate::blobstore::filesystem::FileSystemBlobStoreConfig;
use crate::blobstore::s3::S3BlobStoreConfig;
use crate::metadatastore::etcd::EtcdMetadataStoreConfig;
use crate::metadatastore::filesystem::FileSystemMetadataStoreConfig;

static CONFIG: OnceCell<Config> = OnceCell::new();

impl Config {
    pub fn get() -> &'static Config {
        CONFIG.get().unwrap() // panic intended
    }

    pub fn set(config: Config) -> Result<(), Config> {
        CONFIG.set(config)
    }
}

#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Repository {
    pub name: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen_port: u16,
    pub blob_store: BlobStoreConfig,
    pub metadata_store: MetadataStoreConfig,
    pub repositories: Vec<Repository>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum BlobStoreConfig {
    S3(S3BlobStoreConfig),
    FileSystem(FileSystemBlobStoreConfig),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum MetadataStoreConfig {
    Etcd(EtcdMetadataStoreConfig),
    FileSystem(FileSystemMetadataStoreConfig),
}
