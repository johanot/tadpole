#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

extern crate clap;

use dbc_rust_modules::log;
use warp::Filter;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

use crate::blobstore::filesystem::FileSystemBlobStore;
use crate::blobstore::{BlobError, BlobSpec, BlobStore, ToBlobStore};
use crate::metadatastore::{Manifest, ManifestSpec, ImageRef, MetadataStore, ToMetadataStore, MetadataError};
use crate::metadatastore::filesystem::FileSystemMetadataStore;
use crate::config::Config;
use crate::config::MetadataStoreConfig;
use crate::types::Digest;

use warp::hyper::body::Bytes;

use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;

use serde_json::json;

use warp::http::{Response, StatusCode};
use warp::log::Info;
use warp::path;

mod blobstore;
mod metadatastore;
mod config;
mod types;
/*
mod metrics;
*/

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize, Debug)]
struct RequestLog {
    //remote_addr: String,
    method: String,
    path: String,
    status: u16,
    elapsed: u128,
}

#[derive(Deserialize, Serialize)]
pub struct MonolithicUpload {
    pub digest: Option<String>,
    pub _state: Option<String>,
    pub state: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    log::init(APP_NAME.to_string()).unwrap();

    let args = clap::App::new(APP_NAME)
        .arg(
            clap::Arg::with_name("config-file")
                .long("config-file")
                .help("Path to e8mitter config file")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("config-check")
                .long("config-check")
                .help("If set, the config file will be parsed and the application exits")
                .takes_value(false)
                .required(false),
        );

    let m = args.get_matches();

    let config_path = Path::new(m.value_of("config-file").unwrap());
    let file = File::open(config_path).unwrap();
    let reader = BufReader::new(file);
    let config: Config = serde_json::from_reader(reader).unwrap();

    if m.is_present("config-check") {
        std::process::exit(0);
    }

    Config::set(config).unwrap();

    let (warp_sender, warp_receiver) = oneshot::channel();
    let mut signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    let signal_thread = thread::spawn(move || {
        for sig in signals.forever() {
            log::data("Received signal", &json!({ "signal": &sig }));
            log::info(
                "gracefully wait 1 secs to ensure load balancer sync before shutting down warp",
            );
            thread::sleep(Duration::from_secs(1));
            warp_sender.send(()).unwrap_or_else(|e| {
                log::error("failed to signal warp", &e);
            });
            break;
        }
    });

    listen(warp_receiver);
    signal_thread.join().unwrap();
}

use reqwest::header::HeaderMap;

fn listen(receiver: tokio::sync::oneshot::Receiver<()>) {
    let config = Config::get();
    let listen_port = config.listen_port;

    pretty_env_logger::init();

    log::info(&format!("start listening on port: {}", listen_port));

    let log = warp::log("example::api");

    let headers = warp::header::headers_cloned().map(|headers: HeaderMap| {
        log::info(&format!("headers: {:?}", &headers));
        headers
    });

    let patch_upload = warp::patch()
        .and(path!("v2" / String / "blobs" / "stream" / String))
        .and(headers)
        .and(warp::query())
        .and(warp::filters::body::bytes())
        .and_then(patch_upload)
        .with(warp::compression::gzip());

    let complete_upload = warp::put()
        .and(path!("v2" / String / "blobs" / "stream" / String))
        .and(headers)
        .and(warp::query::<MonolithicUpload>())
        .and(warp::filters::body::bytes())
        .and_then(complete_upload)
        .with(warp::compression::gzip());

    let put_manifests = warp::put()
        .and(path!("v2" / String / "manifests" / ImageRef))
        .and(warp::filters::body::bytes())
        .and_then(put_manifests)
        .with(warp::compression::gzip());

    let upload_check = warp::get()
        .and(path!("v2" / String / "blobs" / "stream" / String))
        .map(upload_check);

    /*let host = warp::host::optional().map(|authority: Option<Authority>| {
        if let Some(a) = authority {
            format!("{} is currently not at home", a.host())
        } else {
            "please state who you're trying to reach".to_owned()
        }
    });*/

    let version_check = warp::get().and(path!("v2")).map(|| {
        Response::builder()
            .header("Docker-Distribution-API-Version", "registry/v2.0")
            .body("")
    });

    let start_upload_blob = warp::post()
        .and(path!("v2" / String / "blobs" / "uploads"))
        .map(start_upload_blob);
    let head_blob = warp::head()
        .and(path!("v2" / String / "blobs" / Digest))
        .and_then(head_blob);
    let get_blob = warp::get()
        .and(path!("v2" / String / "blobs" / Digest))
        .and_then(get_blob);
    let head_manifests = warp::head()
        .and(path!("v2" / String / "manifests" / ImageRef))
        .and_then(head_manifests);
    let get_manifests = warp::get()
        .and(path!("v2" / String / "manifests" / ImageRef))
        .and_then(get_manifests);

    let routes = version_check
        .or(start_upload_blob)
        .or(head_blob)
        .or(get_blob)
        .or(patch_upload)
        .or(complete_upload)
        .or(upload_check)
        .or(head_manifests)
        .or(get_manifests)
        .or(put_manifests)
        .with(log);
    //let get_blob = warp::get().and(path!("v2" / String / "blobs" / String)).map(start_upload_blob);
    //let metrics = warp::path("metrics").map(metrics::serve);
    //let readiness = warp::path("readiness").map(readiness);

    let (_, server) =
        warp::serve(routes).bind_with_graceful_shutdown(([0, 0, 0, 0], listen_port), async move {
            receiver.await.unwrap();
            log::info("shutting down warp")
        });

    tokio::task::spawn(server);
}

async fn head_blob(repo: String, digest: Digest) -> Result<impl warp::Reply, Infallible> {
    let config = Config::get();
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();

    let builder = Response::builder().header("Docker-Distribution-API-Version", "registry/v2.0");

    let builder = match blob_store.stat(BlobSpec { digest }).await {
        Ok(info) => builder
            .status(StatusCode::OK)
            .header("Content-Length", info.size.to_string())
            .header("Content-Type", info.content_type),
        Err(BlobError::NotFound) => builder.status(StatusCode::NOT_FOUND),
        Err(_) => builder.status(StatusCode::INTERNAL_SERVER_ERROR),
    };

    Ok(builder.body(""))
}

async fn get_blob(repo: String, digest: Digest) -> Result<impl warp::Reply, Infallible> {
    let config = Config::get();
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();

    let builder = Response::builder().header("Docker-Distribution-API-Version", "registry/v2.0");

    Ok(match blob_store.get(BlobSpec { digest }).await {
        Ok(mut blob) => builder
            .status(StatusCode::OK)
            .header("Content-Length", blob.info.size.to_string())
            .header("Content-Type", blob.info.content_type)
            .body(blob.body.take().unwrap()),
        Err(BlobError::NotFound) => builder.status(StatusCode::NOT_FOUND).body("".into()),
        Err(_) => builder.status(StatusCode::INTERNAL_SERVER_ERROR).body("".into()),
    })
}

fn start_upload_blob(name: String) -> impl warp::Reply {
    /*
        202 Accepted
        Location: /v2/<name>/blobs/uploads/<uuid>
        Range: bytes=0-0
        Content-Length: 0
        Docker-Upload-UUID: <uuid>
    */
    let config = Config::get();
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();

    let upload_id = blob_store.start_upload().unwrap();
    let uid_str = upload_id.to_string();

    Response::builder()
        .header(
            "Location",
            format!(
                "/v2/{name}/blobs/stream/{uuid}",
                name = &name,
                uuid = &uid_str
            ),
        )
        .header("Range", "0-0")
        .header("Content-Length", "0")
        .header("Docker-Distribution-API-Version", "registry/v2.0")
        .header("Docker-Upload-UUID", &uid_str)
        .status(StatusCode::ACCEPTED)
        .body("")
}

use futures::stream::StreamExt;

use std::convert::Infallible;

use std::iter::Iterator;

async fn patch_upload(
    name: String,
    uuid: String,
    _headers: HeaderMap,
    _mu: MonolithicUpload,
    input: Bytes,
) -> Result<impl warp::Reply, Infallible> {
    let config = Config::get();
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();

    let total = blob_store.patch(&uuid, input).await.unwrap();

    Ok(Response::builder()
        .header(
            "Location",
            format!("/v2/{name}/blobs/stream/{uuid}", name = &name, uuid = &uuid),
        )
        .header("Range", format!("0-{}", total))
        .header("Content-Length", 0)
        .header("Docker-Upload-UUID", &uuid.to_string())
        .status(StatusCode::ACCEPTED)
        .body(""))
}

async fn complete_upload(
    name: String,
    uuid: String,
    _headers: HeaderMap,
    mu: MonolithicUpload,
    input: Bytes,
) -> Result<impl warp::Reply, Infallible> {
    let config = Config::get();
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();

    if !input.is_empty() {
        log::info("Additional bytes to write in the complete_upload step");
        blob_store.patch(&uuid, input).await.unwrap();
    }
    use std::str::FromStr;

    let input_digest = Digest::from_str(&mu.digest.as_ref().unwrap()).unwrap();
    let store_digest = blob_store.get_upload_digest(&uuid).unwrap();

    log::info(&format!(
        "hash comparison: {:?}, {:?}",
        &store_digest, &input_digest
    ));

    if input_digest != store_digest {
        panic!("hash mismatch");
    }

    let _total = blob_store
        .complete_upload(
            &uuid,
            &input_digest
        )
        .await
        .unwrap();

    Ok(Response::builder()
        .header(
            "Location",
            format!(
                "/v2/{name}/blobs/{digest}",
                name = &name,
                digest = &format!("{}", mu.digest.as_ref().unwrap())
            ),
        )
        .header("Content-Length", 0)
        .header("Docker-Upload-UUID", &uuid.to_string())
        .header("Docker-Content-Digest", mu.digest.as_ref().unwrap())
        .status(StatusCode::ACCEPTED)
        .body(""))
}

async fn put_manifests(
    name: String,
    tag: ImageRef,
    input: Bytes,
) -> Result<impl warp::Reply, Infallible> {
    let config = Config::get();
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();

    let digest = Digest::from_bytes(&input);

    let upload_id = blob_store.start_upload().unwrap();
    blob_store.patch(&upload_id, input).await.unwrap();
    blob_store
        .complete_upload(&upload_id, &digest)
        .await
        .unwrap();

    let metadata_store: FileSystemMetadataStore = config
        .metadata_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_metadata_store();
    
    metadata_store.write_spec(&ManifestSpec{
        name: name.clone(),
        reference: tag.clone(),
    }, &digest).unwrap();

    Ok(Response::builder()
        .header(
            "Location",
            format!(
                "/v2/{name}/manifests/{tag}",
                name = &name,
                tag = &tag.to_string()
            ),
        )
        .header("Content-Length", 0)
        .header("Docker-Distribution-API-Version", "registry/v2.0")
        .header("Docker-Content-Digest", &digest.to_typefixed_string())
        .status(StatusCode::CREATED)
        .body(""))
}

fn upload_check(name: String, uuid: String) -> impl warp::Reply {
    Response::builder()
        .header(
            "Location",
            format!("/v2/{name}/blobs/stream/{uuid}", name = &name, uuid = &uuid),
        )
        .header("Range", "0-0")
        .header("Docker-Distribution-API-Version", "registry/v2.0")
        .header("Docker-Upload-UUID", &uuid.to_string())
        .status(StatusCode::NO_CONTENT)
        .body("")
}

async fn manifest(name: String, tag: ImageRef) -> Result<Manifest, ManifestError> {
    let config = Config::get();

    let metadata_store: FileSystemMetadataStore = config
        .metadata_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_metadata_store();
  
    let blob_store: FileSystemBlobStore = config
        .blob_store
        .filesystem
        .as_ref()
        .unwrap()
        .to_blob_store();
  
    let digest = metadata_store.read_spec(&ManifestSpec{
        name: name.clone(),
        reference: tag.clone(),
    }).map_err(std::convert::Into::<ManifestError>::into)?;

    blob_store.get(BlobSpec{ digest: digest.clone() }).await.map_err(std::convert::Into::<ManifestError>::into)
}

async fn head_manifests(name: String, tag: ImageRef) -> Result<impl warp::Reply, Infallible> {
    Ok(manifest(name, tag).await
        .to_response()
        .empty())
}

async fn get_manifests(name: String, tag: ImageRef) -> Result<impl warp::Reply, Infallible> {
    Ok(manifest(name, tag).await
        .to_response()
        .emit())
}

use warp::http::response::Builder;

fn registry_response() -> Builder {
    Response::builder()
        .header("Docker-Distribution-API-Version", "registry/v2.0")
}

trait ToResponse {
    fn to_response(&mut self) -> Envelope;
}

impl ToResponse for Manifest {
    fn to_response(&mut self) -> Envelope {

        let builder = registry_response()
            .status(StatusCode::OK)
            .header("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
            .header("Content-Length", &self.info.size.to_string())
            .header("Docker-Content-Digest", &self.info.digest.to_typefixed_string());

        Envelope {
            builder: Some(builder),
            body: self.body.take(),
        }
    }
}

#[derive(Debug)]
pub enum ManifestError {
    NotFound,
    Other { inner: Box<dyn std::fmt::Debug> },
}

impl ToResponse for ManifestError {
    fn to_response(&mut self) -> Envelope {
        let status = match self {
            ManifestError::NotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        registry_response().status(status).into()
    }
}

impl <A, B>ToResponse for Result<A, B> where A: ToResponse, B: ToResponse {
    fn to_response(&mut self) -> Envelope {
        match self {
            Ok(a) => a.to_response(),
            Err(b) => b.to_response(),
        }
    }
}

impl std::convert::From<BlobError> for ManifestError {
    fn from(e: BlobError) -> ManifestError {
        match e {
            BlobError::NotFound => ManifestError::NotFound,
            e => ManifestError::Other { inner: Box::new(e) },
        }
    }
}

impl std::convert::From<MetadataError> for ManifestError {
    fn from(e: MetadataError) -> ManifestError {
        match e {
            MetadataError::NotFound => ManifestError::NotFound,
            e => ManifestError::Other { inner: Box::new(e) },
        }
    }
}

impl std::convert::From<Builder> for Envelope {
    fn from(builder: Builder) -> Self {
        Envelope {
            body: Some("".into()),
            builder: Some(builder),
        }
    }
}

use std::sync::Arc;


#[derive(Debug)]
struct Envelope {
    body: Option<warp::hyper::Body>,
    builder: Option<Builder>,
}

impl Envelope {
    fn emit(mut self) -> impl warp::Reply {
        self.builder.take().unwrap().body(self.body.take().unwrap())
    }
    fn empty(mut self) -> impl warp::Reply {
        self.builder.take().unwrap().body("")
    }
}