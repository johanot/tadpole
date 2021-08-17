use std::path::PathBuf;
use crate::metadatastore::{ManifestSpec, MetadataStore, MetadataError, ToMetadataStore};
use std::fs;
use async_trait::async_trait;
use crate::metadatastore::ImageRef;
use crate::types::Digest;
use std::fs::File;
use std::io::Write;
use etcd_rs::{Client, ClientConfig, DeleteRequest, KeyRange, LeaseGrantRequest, PutRequest, RangeRequest, TxnCmp, TxnRequest};
use tokio::task;
use tokio::runtime::Handle;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdMetadataStoreConfig {
    endpoints: Vec<String>,
    prefix: String,
    tls: Option<EtcdTLSConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdTLSConfig {
    ca_cert: PathBuf,
    client_cert: PathBuf,
    client_private_key: PathBuf,
}

pub struct EtcdMetadataStore {
  client: Client,
  config: EtcdMetadataStoreConfig,
}

impl ToMetadataStore<EtcdMetadataStore> for EtcdMetadataStoreConfig {
  fn to_metadata_store(&self) -> EtcdMetadataStore {
    EtcdMetadataStore::init(self.clone()).unwrap()
  }
}

fn get_key_for_reference(prefix: &str, repo: &str, name: &str, reference: &str) -> String {
  format!("{prefix}/tags/{repo}/{name}/{reference}",
    prefix=prefix,
    repo=repo,
    name=name,
    reference=reference)
}


impl EtcdMetadataStore {
  fn init(config: EtcdMetadataStoreConfig) -> Result<Self, MetadataError> {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};
    use std::fs;

    let endpoints = config.endpoints.clone();
    let tls = match &config.tls {
        Some(config) => {
            let ca_cert_bytes = fs::read(&config.ca_cert).map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;
            let client_cert_bytes = fs::read(&config.client_cert).map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;
            let private_key_bytes = fs::read(&config.client_private_key).map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;

            let identity = Identity::from_pem(client_cert_bytes, private_key_bytes);
            let ca_cert = Certificate::from_pem(ca_cert_bytes);

            let tls_config = ClientTlsConfig::new();
            let tls_config = tls_config.ca_certificate(ca_cert);
            let tls_config = tls_config.identity(identity);
            Some(tls_config)
        },
        None => None
    };

    let client = task::block_in_place(move || {
      Handle::current().block_on(async move {
        Client::connect(ClientConfig {
          endpoints,
          auth: None, // no support for etcd basic auth yet
          tls
        }).await.map_err(|e| MetadataError::Other{ inner: Box::new(e) })
      })
    })?;

    Ok(Self { client, config })
  }
}

#[async_trait]
impl MetadataStore for EtcdMetadataStore {

  async fn write_spec(&self, spec: &ManifestSpec, manifest_digest: &Digest) -> Result<(), MetadataError> {
    let mut kv_client = self.client.kv();

    let put_req = PutRequest::new(get_key_for_reference(
      &self.config.prefix, 
      &spec.repo.name,
      &spec.name,
      &spec.reference.to_string()), manifest_digest.to_typefixed_string());

    kv_client.put(put_req).await.map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;
    Ok(())
  }

  async fn read_spec(&self, spec: &ManifestSpec) -> Result<Digest, MetadataError> {
    use std::str::FromStr;

    match &spec.reference {
      ImageRef::Digest(d) => Ok(d.clone()),
      ImageRef::Tag(t) => {
        let mut kv_client = self.client.kv();
        let req = RangeRequest::new(KeyRange::key(get_key_for_reference(
          &self.config.prefix, 
          &spec.repo.name,
          &spec.name,
          &spec.reference.to_string())));
        
        let mut res = kv_client.range(req).await.map_err(|e| MetadataError::Other{ inner: Box::new(e) })?;
        let count = res.count();
        match count {
          c if c <=1 => {
            res.take_kvs().iter().next().ok_or(MetadataError::NotFound).and_then(|v| {
              Digest::from_str(v.value_str()).map_err(|e| MetadataError::Other{ inner: Box::new(e) })
            })
          },
          _ => Err(MetadataError::Ambiguous)
        }
      }
    }
  }
}