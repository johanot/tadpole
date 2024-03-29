use reqwest::header::HeaderValue;
use sha2::Digest as Sha2Digest;
use sha2::Sha256;
use std::str::FromStr;
use std::io::BufRead;

use warp::hyper::body::Bytes;
use std::sync::mpsc::Receiver;

use dbc_rust_modules::log;

#[derive(Clone, Debug, Serialize)]
pub enum ContentType {
    OctetStream,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Digest {
    pub algo: DigestAlgo,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, strum::ToString, strum::EnumString, PartialEq, Serialize)]
#[strum(serialize_all = "lowercase")]
pub enum DigestAlgo {
    Sha256,
}

impl Default for ContentType {
    fn default() -> Self {
        Self::OctetStream
    }
}

impl Default for DigestAlgo {
    fn default() -> Self {
        Self::Sha256
    }
}

impl std::convert::From<&ContentType> for HeaderValue {
    fn from(content_type: &ContentType) -> Self {
        Self::from_static(match content_type {
            ContentType::OctetStream => "application/octet-stream",
        })
    }
}

impl std::fmt::LowerHex for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.value {
            write!(f, "{:02x}", b)?
        }
        Ok(())
    }
}

//TODO: make generic based on algo
impl Digest {
    pub fn to_standalone_string(&self) -> String {
        format!("{:x}", &self)
    }

    pub fn to_typefixed_string(&self) -> String {
        format!("{}:{:x}", &self.algo.to_string(), &self)
    }

    pub fn from_bytes(input: &Bytes) -> Self {
        //TODO: hardcoded until further algorithms are support, consider generifying then
        let algo = DigestAlgo::Sha256;
        let mut hasher = Sha256::new();
        hasher.update(&input.to_vec());
        Self {
            algo,
            value: hasher.finalize().to_vec(),
        }
    }

    pub fn from_reader<T: BufRead>(mut reader: T) -> Result<Self, DigestParseError> {
        //TODO: hardcoded until further algorithms are support, consider generifying then
        let algo = DigestAlgo::Sha256;
        let mut hasher = Sha256::new();
        loop {
            let buffer = reader.fill_buf().unwrap();
            let buf_len = buffer.len();
            if buf_len > 0 {
                hasher.update(buffer);
                reader.consume(buf_len);
            } else {
                break;
            }
        }
        Ok(Self {
            algo,
            value: hasher.finalize().to_vec(),
        })
    }

    pub fn from_channel(receiver: Receiver<Bytes>) -> Result<Self, DigestParseError> {
        //TODO: hardcoded until further algorithms are support, consider generifying then
        let algo = DigestAlgo::Sha256;
        let mut hasher = Sha256::new();
        while let Some(item) = receiver.recv().ok() {
            hasher.update(item);
        }
        Ok(Self {
            algo,
            value: hasher.finalize().to_vec(),
        })
    }
}

#[derive(Debug)]
pub enum DigestParseError {
    FormatError,
    UnknownAlgorithm,
    MalformedValue,
}

impl FromStr for Digest {
    type Err = DigestParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(":");
        let algo = parts
            .next()
            .ok_or(DigestParseError::FormatError)
            .and_then(|d| {
                DigestAlgo::from_str(d).map_err(|_| DigestParseError::UnknownAlgorithm)
            })?;

        let value_string = parts.next().ok_or(DigestParseError::FormatError)?;
        //TODO: check digest length as well, some day
        let value = hex::decode(value_string).map_err(|_| DigestParseError::MalformedValue)?;

        if parts.next().is_none() {
            Ok(Digest { algo, value })
        } else {
            Err(DigestParseError::FormatError)
        }
    }
}
