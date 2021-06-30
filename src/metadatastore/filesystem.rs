use std::path::PathBuf;
use crate::metadatastore::{ManifestSpec, MetadataStore, MetadataError, ToMetadataStore};
use std::fs;
use async_trait::async_trait;
use crate::metadatastore::ImageRef;
use crate::types::Digest;
use std::fs::File;
use std::io::Write;

#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSystemMetadataStoreConfig {
    tags_path: PathBuf,
    //TODO: moar fields
}

pub struct FileSystemMetadataStore {
    config: FileSystemMetadataStoreConfig,
}

impl ToMetadataStore<FileSystemMetadataStore> for FileSystemMetadataStoreConfig {
  fn to_metadata_store(&self) -> FileSystemMetadataStore {
    FileSystemMetadataStore::init(self.clone()).unwrap()
  }
}

#[async_trait]
impl MetadataStore<FileSystemMetadataStoreConfig> for FileSystemMetadataStore {
  fn init(config: FileSystemMetadataStoreConfig) -> Result<Self, MetadataError> {
    fs::create_dir_all(&config.tags_path)
        .map_err(|e| MetadataError::Other { inner: Box::new(e) })?;
    Ok(Self { config })
  }

  fn write_spec(&self, spec: &ManifestSpec, manifest_digest: &Digest) -> Result<(), MetadataError> {
    let fqp = self.config.tags_path.join(&spec.name);
    match fs::create_dir(&fqp) {
      Ok(()) => Ok(()),
      Err(e) => if e.kind() == std::io::ErrorKind::AlreadyExists {
        Ok(())
      } else {
        Err(MetadataError::Other{ inner: Box::new(e) })
      }
    }?;

    let mut file = File::create(&fqp.join(&spec.reference.to_string())).map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;
    file.write_all(manifest_digest.to_typefixed_string().as_bytes()).map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;
    Ok(())
  }

  fn read_spec(&self, spec: &ManifestSpec) -> Result<Digest, MetadataError> {
    use std::str::FromStr;

    match &spec.reference {
      ImageRef::Digest(d) => Ok(d.clone()),
      ImageRef::Tag(t) => {
        let fqp = self.config.tags_path.join(&spec.name).join(&t);
        let digest_string = std::fs::read_to_string(&fqp).map_err(|_| MetadataError::NotFound)?;
        Ok(Digest::from_str(&digest_string).map_err(|e| MetadataError::Other{ inner: Box::new(e) })?)
      }
    }
  }
}