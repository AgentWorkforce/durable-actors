use anyhow::{Result, ensure};
use axum::{
    body::Body,
    http::header,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    fs::File,
    io::{self, Read, Write},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub(crate) const BUFFER_BYTES: usize = 64 * 1024;
pub(crate) const IO_BUFFER_BYTES: usize = 256 * 1024;
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

pub(crate) struct Spool {
    memory: Vec<u8>,
    file: Option<tempfile::NamedTempFile>,
    length: u64,
}

impl Spool {
    pub fn new() -> Self {
        Self {
            memory: Vec::new(),
            file: None,
            length: 0,
        }
    }

    pub fn finish(mut self) -> Result<Bytes> {
        self.flush()?;
        match self.file {
            None => Ok(self.memory.into()),
            Some(file) => {
                let path = file.into_temp_path();
                let file = File::open(&path)?;
                // This privately owned file is immutable until the last mapping is dropped.
                let map = unsafe { memmap2::Mmap::map(&file)? };
                Ok(Bytes::from_owner(Mapped { map, _path: path }))
            }
        }
    }
}

impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .filter(|length| *length <= MAX_FILE_BYTES)
            .ok_or_else(|| io::Error::other("temporary storage object exceeds 2 GiB"))?;
        if self.file.is_none() && length > BUFFER_BYTES as u64 {
            let mut file = tempfile::NamedTempFile::new()?;
            file.write_all(&self.memory)?;
            self.memory = Vec::new();
            self.file = Some(file);
        }
        match self.file.as_mut() {
            Some(file) => file.write_all(bytes)?,
            None => self.memory.extend_from_slice(bytes),
        }
        self.length = length;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

struct Mapped {
    map: memmap2::Mmap,
    _path: tempfile::TempPath,
}
impl AsRef<[u8]> for Mapped {
    fn as_ref(&self) -> &[u8] {
        &self.map
    }
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Bytes> {
    let mut spool = Spool::new();
    serde_json::to_writer(&mut spool, value)?;
    spool.finish()
}

pub(crate) fn copy(mut reader: impl Read) -> Result<Bytes> {
    let mut spool = Spool::new();
    io::copy(&mut reader, &mut spool)?;
    spool.finish()
}

#[derive(Clone, Debug)]
pub struct Text(Bytes);

impl Deref for Text {
    type Target = str;
    fn deref(&self) -> &str {
        std::str::from_utf8(&self.0).expect("validated text")
    }
}
impl AsRef<[u8]> for Text {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl From<String> for Text {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}
impl Serialize for Text {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self)
    }
}
impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Text;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("base64 text")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Text, E> {
                copy(value.as_bytes()).map(Text).map_err(E::custom)
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Text, E> {
                self.visit_str(&value)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

/// A JSON document kept as its text, which passes through without being parsed. Clones share the
/// text.
// Boxed because `Arc<RawValue>` would copy the text out of its box.
#[derive(Clone, Debug)]
pub struct JsonText(Arc<Box<RawValue>>);

impl JsonText {
    /// Checks that `bytes` hold one JSON document and keeps them as its text, without a copy.
    pub fn from_vec(bytes: Vec<u8>) -> Result<Self> {
        Ok(RawValue::from_string(String::from_utf8(bytes)?)?.into())
    }

    pub fn encode(value: &impl Serialize) -> Result<Self> {
        Ok(serde_json::value::to_raw_value(value)?.into())
    }

    pub fn null() -> Self {
        RawValue::NULL.to_owned().into()
    }

    pub fn get(&self) -> &str {
        self.0.get()
    }

    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_str(self.get())?)
    }

    /// The text as a buffer that shares it.
    pub fn bytes(&self) -> Bytes {
        Bytes::from_owner(self.clone())
    }
}

impl Default for JsonText {
    fn default() -> Self {
        Self::null()
    }
}
impl From<Box<RawValue>> for JsonText {
    fn from(value: Box<RawValue>) -> Self {
        Self(Arc::new(value))
    }
}
impl From<serde_json::Value> for JsonText {
    fn from(value: serde_json::Value) -> Self {
        serde_json::value::to_raw_value(&value)
            .expect("a JSON value always encodes")
            .into()
    }
}
impl AsRef<[u8]> for JsonText {
    fn as_ref(&self) -> &[u8] {
        self.get().as_bytes()
    }
}
impl PartialEq for JsonText {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}
impl Eq for JsonText {}
impl Serialize for JsonText {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for JsonText {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Box::<RawValue>::deserialize(deserializer).map(Self::from)
    }
}

/// A JSON body sent from shared buffers, so the documents inside it are never copied.
pub(crate) struct JsonBody(VecDeque<Bytes>);

impl JsonBody {
    pub fn encode(value: &impl Serialize) -> Result<Self> {
        Ok(Self(VecDeque::from([serde_json::to_vec(value)?.into()])))
    }

    /// `object` with `value` added as its last `field`.
    pub fn with_field(object: &impl Serialize, field: &str, value: &JsonText) -> Result<Self> {
        let mut head = serde_json::to_vec(object)?;
        ensure!(head.pop() == Some(b'}'), "a JSON body needs an object");
        if head.len() > 1 {
            head.push(b',');
        }
        serde_json::to_writer(&mut head, field)?;
        head.push(b':');
        Ok(Self(VecDeque::from([
            head.into(),
            value.bytes(),
            Bytes::from_static(b"}"),
        ])))
    }

    pub fn into_text(self) -> Result<JsonText> {
        let mut text = Vec::with_capacity(self.len());
        for bytes in &self.0 {
            text.extend_from_slice(bytes);
        }
        JsonText::from_vec(text)
    }

    fn len(&self) -> usize {
        self.0.iter().map(Bytes::len).sum()
    }
}

impl IntoResponse for JsonBody {
    fn into_response(self) -> Response {
        (
            [(header::CONTENT_TYPE, "application/json")],
            Body::new(self),
        )
            .into_response()
    }
}

impl http_body::Body for JsonBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.pop_front().map(|bytes| Ok(Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.len() as u64)
    }
}

pub(crate) struct Upload {
    bytes: Bytes,
    offset: usize,
}
impl Upload {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, offset: 0 }
    }
}
impl google_cloud_storage::streaming_source::StreamingSource for Upload {
    type Error = io::Error;
    async fn next(&mut self) -> Option<io::Result<Bytes>> {
        if self.offset == self.bytes.len() {
            return None;
        }
        let end = self.bytes.len().min(self.offset + IO_BUFFER_BYTES);
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Some(Ok(bytes))
    }
    async fn size_hint(&self) -> io::Result<google_cloud_storage::streaming_source::SizeHint> {
        Ok(google_cloud_storage::streaming_source::SizeHint::with_exact(self.bytes.len() as u64))
    }
}
impl google_cloud_storage::streaming_source::Seek for Upload {
    type Error = io::Error;
    async fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.offset = usize::try_from(offset).map_err(io::Error::other)?;
        if self.offset > self.bytes.len() {
            return Err(io::Error::other("upload seek exceeds object"));
        }
        Ok(())
    }
}

pub(crate) struct Download {
    spool: Spool,
    buffer: Vec<u8>,
}

impl Download {
    pub fn new() -> Self {
        Self {
            spool: Spool::new(),
            buffer: Vec::new(),
        }
    }

    pub async fn append(mut self, bytes: Bytes) -> Result<Self> {
        let mut remaining = bytes.as_ref();
        while !remaining.is_empty() {
            let length = remaining.len().min(IO_BUFFER_BYTES - self.buffer.len());
            let capacity = (self.buffer.len() + length).next_power_of_two();
            self.buffer.reserve_exact(capacity - self.buffer.len());
            self.buffer.extend_from_slice(&remaining[..length]);
            remaining = &remaining[length..];
            if self.buffer.len() == IO_BUFFER_BYTES {
                self = tokio::task::spawn_blocking(move || -> Result<Self> {
                    self.flush()?;
                    Ok(self)
                })
                .await??;
            }
        }
        Ok(self)
    }

    pub async fn finish(mut self) -> Result<Bytes> {
        tokio::task::spawn_blocking(move || {
            self.flush()?;
            self.spool.finish()
        })
        .await?
    }

    fn flush(&mut self) -> io::Result<()> {
        self.spool.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/storage_payload.rs"]
mod tests;
