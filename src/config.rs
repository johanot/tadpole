use once_cell::sync::OnceCell;
use reqwest::Url;
use serde::de::{self, Deserialize, Deserializer};

use crate::blobstore::filesystem::FileSystemBlobStoreConfig;
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

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen_port: u16,
    pub blob_store: BlobStoreConfig,
    pub metadata_store: MetadataStoreConfig,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct BlobStoreConfig {
    pub s3: Option<S3BlobStoreConfig>,
    pub filesystem: Option<FileSystemBlobStoreConfig>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct S3BlobStoreConfig {
    #[serde(deserialize_with = "deserialize_url")]
    pub url: Url,
    //TODO: moar fields
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct MetadataStoreConfig {
    pub etcd: Option<EtcdMetadataStoreConfig>,
    pub filesystem: Option<FileSystemMetadataStoreConfig>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct EtcdMetadataStoreConfig {
    #[serde(deserialize_with = "deserialize_url")]
    pub url: Url,
    //TODO: moar fields
}

fn deserialize_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: Deserializer<'de>,
{
    let url: String = Deserialize::deserialize(deserializer)?;
    Url::parse(&url).map_err(de::Error::custom)
}
