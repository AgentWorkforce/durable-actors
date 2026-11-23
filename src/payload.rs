use anyhow::{Context as _, Result, anyhow, ensure};
use axum::{
    body::Body,
    http::header,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use http_body::{Frame, SizeHint};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::{
    collections::VecDeque,
    fmt,
    fs::File,
    io::{self, Read, Write},
    ops::{Bound, Range, RangeBounds},
    os::unix::fs::FileExt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};
use tokio::sync::oneshot;

pub(crate) const BUFFER_BYTES: usize = 64 * 1024;
/// Results larger than this are passed on as they arrive, rather than held whole.
pub(crate) const ARRIVING_BYTES: usize = 1024 * 1024;
pub(crate) const IO_BUFFER_BYTES: usize = 256 * 1024;

/// Bytes kept in memory, or in a temporary file that is read through bounded buffers so its
/// size never becomes resident memory. Clones and slices share the file.
#[derive(Clone)]
pub enum Payload {
    Memory(Bytes),
    File(FileRange),
}

#[derive(Clone)]
pub struct FileRange {
    file: Arc<File>,
    start: u64,
    len: usize,
}

impl Payload {
    /// The first `len` bytes of a temporary file, read through bounded buffers.
    pub(crate) fn from_file(file: Arc<File>, len: usize) -> Self {
        Self::File(FileRange {
            file,
            start: 0,
            len,
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Memory(bytes) => bytes.len(),
            Self::File(range) => range.len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        match self {
            Self::Memory(bytes) => Self::Memory(bytes.slice(range)),
            Self::File(file) => {
                let range = bounds(range, file.len);
                Self::File(FileRange {
                    file: file.file.clone(),
                    start: file.start + range.start as u64,
                    len: range.len(),
                })
            }
        }
    }

    /// Reads `range` into memory, for pieces small enough to hold.
    pub fn read(&self, range: Range<usize>) -> io::Result<Bytes> {
        match self.slice(range) {
            Self::Memory(bytes) => Ok(bytes),
            Self::File(range) => {
                let mut bytes = vec![0; range.len];
                range.file.read_exact_at(&mut bytes, range.start)?;
                Ok(bytes.into())
            }
        }
    }

    /// Reads all of it into memory, for objects small enough to hold.
    pub fn to_bytes(&self) -> io::Result<Bytes> {
        self.read(0..self.len())
    }

    pub fn starts_with(&self, prefix: &[u8]) -> io::Result<bool> {
        Ok(self.len() >= prefix.len() && self.read(0..prefix.len())? == prefix)
    }

    /// Passes the contents to `visit` in order, reading a file through one bounded buffer.
    pub fn for_each_chunk(&self, mut visit: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
        let Self::File(range) = self else {
            return visit(&self.read(0..self.len())?);
        };
        let mut buffer = vec![0; IO_BUFFER_BYTES.min(range.len)];
        let mut offset = 0;
        while offset < range.len {
            let length = buffer.len().min(range.len - offset);
            (range.file).read_exact_at(&mut buffer[..length], range.start + offset as u64)?;
            visit(&buffer[..length])?;
            offset += length;
        }
        Ok(())
    }

    pub fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        self.for_each_chunk(|chunk| writer.write_all(chunk))
    }

    pub fn reader(&self) -> io::BufReader<PayloadReader> {
        self.reader_with_capacity(BUFFER_BYTES)
    }

    pub fn reader_with_capacity(&self, capacity: usize) -> io::BufReader<PayloadReader> {
        io::BufReader::with_capacity(
            capacity,
            PayloadReader {
                payload: self.clone(),
                position: 0,
            },
        )
    }

    /// Up to `IO_BUFFER_BYTES` from `offset`, with file reads kept off the async runtime.
    pub async fn chunk(&self, offset: usize) -> io::Result<Bytes> {
        let end = self.len().min(offset.saturating_add(IO_BUFFER_BYTES));
        match self {
            Self::Memory(bytes) => Ok(bytes.slice(offset..end)),
            Self::File(_) => {
                let payload = self.clone();
                tokio::task::spawn_blocking(move || payload.read(offset..end))
                    .await
                    .map_err(io::Error::other)?
            }
        }
    }

    pub fn chunks(&self) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
        futures_util::stream::try_unfold((self.clone(), 0), |(payload, offset)| async move {
            if offset == payload.len() {
                return Ok(None);
            }
            let chunk = payload.chunk(offset).await?;
            let next = offset + chunk.len();
            Ok(Some((chunk, (payload, next))))
        })
    }

    fn same_chunks(&self, other: &Self) -> io::Result<bool> {
        let mut offset = 0;
        while offset < self.len() {
            let end = self.len().min(offset + IO_BUFFER_BYTES);
            if self.read(offset..end)? != other.read(offset..end)? {
                return Ok(false);
            }
            offset = end;
        }
        Ok(true)
    }
}

fn bounds(range: impl RangeBounds<usize>, len: usize) -> Range<usize> {
    let start = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(end) => end + 1,
        Bound::Excluded(end) => *end,
        Bound::Unbounded => len,
    };
    assert!(
        start <= end && end <= len,
        "range {start}..{end} out of bounds for {len} bytes"
    );
    start..end
}

impl Default for Payload {
    fn default() -> Self {
        Self::Memory(Bytes::new())
    }
}
impl From<Bytes> for Payload {
    fn from(bytes: Bytes) -> Self {
        Self::Memory(bytes)
    }
}
impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Memory(bytes.into())
    }
}
impl From<&'static [u8]> for Payload {
    fn from(bytes: &'static [u8]) -> Self {
        Self::Memory(Bytes::from_static(bytes))
    }
}
/// Compares contents, reading files; a failed read counts as a difference.
impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            _ if self.len() != other.len() => false,
            (Self::Memory(left), Self::Memory(right)) => left == right,
            (Self::File(left), Self::File(right))
                if Arc::ptr_eq(&left.file, &right.file) && left.start == right.start =>
            {
                true
            }
            _ => self.same_chunks(other).unwrap_or(false),
        }
    }
}
impl fmt::Debug for Payload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(bytes) => bytes.fmt(formatter),
            Self::File(range) => formatter
                .debug_struct("File")
                .field("len", &range.len)
                .finish_non_exhaustive(),
        }
    }
}

pub struct PayloadReader {
    payload: Payload,
    position: usize,
}

impl Read for PayloadReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let length = buffer.len().min(self.payload.len() - self.position);
        let target = &mut buffer[..length];
        match &self.payload {
            Payload::Memory(bytes) => {
                target.copy_from_slice(&bytes[self.position..self.position + length])
            }
            Payload::File(range) => {
                (range.file).read_exact_at(target, range.start + self.position as u64)?
            }
        }
        self.position += length;
        Ok(length)
    }
}

pub(crate) struct Spool {
    memory: Vec<u8>,
    file: Option<File>,
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

    pub fn finish(self) -> Result<Payload> {
        Ok(match self.file {
            None => Payload::Memory(self.memory.into()),
            Some(file) => Payload::File(FileRange {
                file: Arc::new(file),
                start: 0,
                len: usize::try_from(self.length)?,
            }),
        })
    }
}

impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("temporary storage object overflow"))?;
        if self.file.is_none() && length > BUFFER_BYTES as u64 {
            let mut file = tempfile::tempfile()?;
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
        Ok(())
    }
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Payload> {
    let mut spool = Spool::new();
    serde_json::to_writer(&mut spool, value)?;
    spool.finish()
}

pub(crate) fn copy(mut reader: impl Read) -> Result<Payload> {
    let mut spool = Spool::new();
    io::copy(&mut reader, &mut spool)?;
    spool.finish()
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

/// A JSON document that is either whole or still arriving.
pub enum JsonPayload {
    Whole(JsonText),
    Arriving(JsonStream),
}

impl JsonPayload {
    pub fn whole(&self) -> Option<&JsonText> {
        match self {
            Self::Whole(text) => Some(text),
            Self::Arriving(_) => None,
        }
    }

    pub async fn into_text(self) -> Result<JsonText> {
        match self {
            Self::Whole(text) => Ok(text),
            Self::Arriving(stream) => stream.into_text().await,
        }
    }
}

impl Default for JsonPayload {
    fn default() -> Self {
        Self::Whole(JsonText::null())
    }
}
impl From<JsonText> for JsonPayload {
    fn from(text: JsonText) -> Self {
        Self::Whole(text)
    }
}
impl From<JsonStream> for JsonPayload {
    fn from(stream: JsonStream) -> Self {
        Self::Arriving(stream)
    }
}
impl From<serde_json::Value> for JsonPayload {
    fn from(value: serde_json::Value) -> Self {
        JsonText::from(value).into()
    }
}
impl fmt::Debug for JsonPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Whole(text) => text.fmt(formatter),
            Self::Arriving(stream) => stream.fmt(formatter),
        }
    }
}
/// A document still arriving equals nothing, since its text is not known yet.
impl PartialEq for JsonPayload {
    fn eq(&self, other: &Self) -> bool {
        matches!((self.whole(), other.whole()), (Some(text), Some(other)) if text == other)
    }
}

/// A JSON document's text as it arrives, in chunks that add up to a length announced before them.
pub struct JsonStream {
    chunks: BoxStream<'static, Result<Bytes>>,
    remaining: usize,
    finished: Option<oneshot::Sender<()>>,
}

impl JsonStream {
    pub fn new(chunks: impl Stream<Item = Result<Bytes>> + Send + 'static, len: usize) -> Self {
        Self {
            chunks: chunks.boxed(),
            remaining: len,
            finished: None,
        }
    }

    /// Resolves once the document has been passed on or dropped.
    pub fn finished(&mut self) -> oneshot::Receiver<()> {
        let (finished, receiver) = oneshot::channel();
        self.finished = Some(finished);
        receiver
    }

    async fn into_text(self) -> Result<JsonText> {
        let mut text = Vec::new();
        text.try_reserve_exact(self.remaining)?;
        self.read_into(&mut text).await?;
        JsonText::from_vec(text)
    }

    async fn read_into(mut self, text: &mut Vec<u8>) -> Result<()> {
        while let Some(chunk) = std::future::poll_fn(|context| self.poll_chunk(context)).await {
            text.extend_from_slice(&chunk?);
        }
        Ok(())
    }

    fn poll_chunk(&mut self, context: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        let chunk = ready!(self.chunks.poll_next_unpin(context))
            .unwrap_or_else(|| Err(anyhow!("a JSON document ended before all of it arrived")))
            .and_then(|chunk| {
                self.remaining = (self.remaining.checked_sub(chunk.len()))
                    .context("a JSON document arrived longer than announced")?;
                Ok(chunk)
            });
        Poll::Ready(Some(chunk))
    }
}

impl fmt::Debug for JsonStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonStream")
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

/// A JSON body sent from shared buffers and arriving documents, so the documents inside it are
/// never copied.
pub(crate) struct JsonBody(VecDeque<Piece>);

impl JsonBody {
    pub fn encode(value: &impl Serialize) -> Result<Self> {
        Ok(Self(VecDeque::from([Piece::Ready(
            serde_json::to_vec(value)?.into(),
        )])))
    }

    /// `object` with `value` added as its last `field`.
    pub fn with_field(object: &impl Serialize, field: &str, value: JsonPayload) -> Result<Self> {
        let mut head = serde_json::to_vec(object)?;
        ensure!(head.pop() == Some(b'}'), "a JSON body needs an object");
        if head.len() > 1 {
            head.push(b',');
        }
        serde_json::to_writer(&mut head, field)?;
        head.push(b':');
        let value = match value {
            JsonPayload::Whole(text) => Piece::Ready(text.bytes()),
            JsonPayload::Arriving(stream) => Piece::Arriving(stream),
        };
        Ok(Self(VecDeque::from([
            Piece::Ready(head.into()),
            value,
            Piece::Ready(Bytes::from_static(b"}")),
        ])))
    }

    pub async fn into_text(self) -> Result<JsonText> {
        let mut text = Vec::new();
        text.try_reserve_exact(self.len())?;
        for piece in self.0 {
            match piece {
                Piece::Ready(bytes) => text.extend_from_slice(&bytes),
                Piece::Arriving(stream) => stream.read_into(&mut text).await?,
            }
        }
        JsonText::from_vec(text)
    }

    fn len(&self) -> usize {
        self.0.iter().map(Piece::len).sum()
    }

    fn poll_bytes(&mut self, context: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        while let Some(piece) = self.0.front_mut() {
            match ready!(piece.poll_next(context)) {
                Some(bytes) => return Poll::Ready(Some(bytes)),
                None => drop(self.0.pop_front()),
            }
        }
        Poll::Ready(None)
    }
}

enum Piece {
    Ready(Bytes),
    Arriving(JsonStream),
}

impl Piece {
    fn len(&self) -> usize {
        match self {
            Self::Ready(bytes) => bytes.len(),
            Self::Arriving(stream) => stream.remaining,
        }
    }

    /// The piece's next bytes, or `None` once all of them are sent.
    fn poll_next(&mut self, context: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self {
            Self::Ready(bytes) if bytes.is_empty() => Poll::Ready(None),
            Self::Ready(bytes) => Poll::Ready(Some(Ok(std::mem::take(bytes)))),
            Self::Arriving(stream) => stream.poll_chunk(context),
        }
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
    type Error = anyhow::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>>>> {
        self.poll_bytes(context)
            .map(|bytes| bytes.map(|bytes| bytes.map(Frame::data)))
    }

    fn is_end_stream(&self) -> bool {
        self.len() == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.len() as u64)
    }
}

impl From<JsonBody> for reqwest::Body {
    fn from(mut body: JsonBody) -> Self {
        Self::wrap_stream(futures_util::stream::poll_fn(move |context| {
            body.poll_bytes(context)
        }))
    }
}

pub(crate) struct Upload {
    payload: Payload,
    offset: usize,
}
impl Upload {
    pub fn new(payload: Payload) -> Self {
        Self { payload, offset: 0 }
    }
}
impl google_cloud_storage::streaming_source::StreamingSource for Upload {
    type Error = io::Error;
    async fn next(&mut self) -> Option<io::Result<Bytes>> {
        if self.offset == self.payload.len() {
            return None;
        }
        let chunk = self.payload.chunk(self.offset).await;
        if let Ok(chunk) = &chunk {
            self.offset += chunk.len();
        }
        Some(chunk)
    }
    async fn size_hint(&self) -> io::Result<google_cloud_storage::streaming_source::SizeHint> {
        Ok(google_cloud_storage::streaming_source::SizeHint::with_exact(self.payload.len() as u64))
    }
}
impl google_cloud_storage::streaming_source::Seek for Upload {
    type Error = io::Error;
    async fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.offset = usize::try_from(offset).map_err(io::Error::other)?;
        if self.offset > self.payload.len() {
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

    pub async fn finish(mut self) -> Result<Payload> {
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
