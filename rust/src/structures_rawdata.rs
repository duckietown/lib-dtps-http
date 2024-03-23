use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha256::digest;

use crate::{
    identify_presentation, invalid_input, ContentPresentation, DTPSError, RawData, CONTENT_TYPE_CBOR,
    CONTENT_TYPE_JSON, CONTENT_TYPE_OCTET_STREAM, DTPSR,
};

impl RawData {
    pub fn new<S: AsRef<[u8]>, T: AsRef<str>>(content: S, content_type: T) -> RawData {
        RawData {
            content: Bytes::from(content.as_ref().to_vec()),
            content_type: content_type.as_ref().to_string(),
        }
    }
    pub fn cbor<S: AsRef<[u8]>>(content: S) -> RawData {
        Self::new(content, CONTENT_TYPE_CBOR)
    }
    pub fn json<S: AsRef<[u8]>>(content: S) -> RawData {
        Self::new(content, CONTENT_TYPE_JSON)
    }
    pub fn digest(&self) -> String {
        let d = digest(self.content.as_ref());
        format!("sha256:{}", d)
    }

    pub fn represent_as_json<T: Serialize>(x: T) -> DTPSR<Self> {
        Ok(RawData::new(serde_json::to_vec(&x)?, CONTENT_TYPE_JSON))
    }
    pub fn represent_as_cbor<T: Serialize>(x: T) -> DTPSR<Self> {
        Ok(RawData::new(serde_cbor::to_vec(&x)?, CONTENT_TYPE_CBOR))
    }
    pub fn represent_as_cbor_ct<T: Serialize>(x: T, ct: &str) -> DTPSR<Self> {
        Ok(RawData::new(serde_cbor::to_vec(&x)?, ct))
    }
    pub fn from_cbor_value(x: &serde_cbor::Value) -> DTPSR<Self> {
        Ok(RawData::cbor(serde_cbor::to_vec(x)?))
    }
    pub fn from_json_value(x: &serde_json::Value) -> DTPSR<Self> {
        Ok(RawData::json(serde_json::to_vec(x)?))
    }
    /// Deserializes the content of this object as a given type.
    /// The type must implement serde::Deserialize.
    /// It works only for CBOR, JSON and YAML.
    pub fn interpret<'a, T>(&'a self) -> DTPSR<T>
    where
        T: Deserialize<'a> + Clone,
    {
        match self.presentation() {
            ContentPresentation::CBOR => {
                let v: T = serde_cbor::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::JSON => {
                let v: T = serde_json::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::YAML => {
                let v: T = serde_yaml::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::PlainText => DTPSError::other("cannot interpret plain text"),
            ContentPresentation::Other => {
                let s = format!(
                    "cannot interpret unknown content type {:?} {:?}",
                    self.content_type, self.content
                );
                DTPSError::other(s)
            }
        }
    }

    pub fn interpret_owned<T>(&self) -> DTPSR<T>
    where
        T: DeserializeOwned + Clone,
    {
        match self.presentation() {
            ContentPresentation::CBOR => {
                let v: T = serde_cbor::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::JSON => {
                let v: T = serde_json::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::YAML => {
                let v: T = serde_yaml::from_slice::<T>(&self.content)?;
                Ok(v)
            }
            ContentPresentation::PlainText => invalid_input!("cannot interpret plain text"),
            ContentPresentation::Other => {
                invalid_input!("cannot interpret unknown content type, {}", self.content_type)
            }
        }
    }

    pub fn presentation(&self) -> ContentPresentation {
        identify_presentation(&self.content_type)
    }

    pub fn try_translate(&self, ct: &str) -> DTPSR<Self> {
        if self.content_type == ct {
            return Ok(self.clone());
        }
        if ct == CONTENT_TYPE_OCTET_STREAM {
            return Ok(RawData::new(self.content.clone(), ct));
        }
        let mine = identify_presentation(self.content_type.as_str());
        let desired = identify_presentation(ct);

        let value = self.get_as_cbor()?;
        let bytes = match desired {
            ContentPresentation::CBOR => serde_cbor::to_vec(&value)?,
            ContentPresentation::JSON => serde_json::to_vec(&value)?,
            ContentPresentation::YAML => Bytes::from(serde_yaml::to_string(&value)?).to_vec(),
            ContentPresentation::PlainText | ContentPresentation::Other => {
                return DTPSError::other(format!(
                    "cannot translate from {:?} to {:?}, classified as {:?}",
                    mine, ct, desired
                ));
            }
        };
        Ok(RawData::new(bytes, ct))
    }
}
