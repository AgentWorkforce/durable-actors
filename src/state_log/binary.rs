//! DAS2: a JSON header naming the LTX files, then their raw bytes, so state skips base64.
use crate::litestream::storage::LtxFile;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::Write;

const MAGIC: &[u8; 4] = b"DAS2";
const PREFIX: usize = MAGIC.len() + 4;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    value: T,
    files: Vec<FileHeader>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileHeader {
    level: u8,
    first: u64,
    last: u64,
    bytes: u64,
}

pub(crate) fn is_binary(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

/// `value` carries everything but the bytes of `files`.
pub(crate) fn encode<T: Serialize>(value: &T, files: &[LtxFile]) -> Result<Bytes> {
    let header = serde_json::to_vec(&Envelope {
        value,
        files: files.iter().map(FileHeader::new).collect(),
    })?;
    let length = u32::try_from(header.len()).context("state header too large")?;
    let mut spool = crate::payload::Spool::new();
    spool.write_all(MAGIC)?;
    spool.write_all(&length.to_le_bytes())?;
    spool.write_all(&header)?;
    for file in files {
        spool.write_all(&file.data)?;
    }
    spool.finish()
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<(T, Vec<LtxFile>)> {
    ensure!(
        is_binary(bytes) && bytes.len() >= PREFIX,
        "not a binary state object"
    );
    let length = u32::from_le_bytes(bytes[MAGIC.len()..PREFIX].try_into()?) as usize;
    let start = PREFIX
        .checked_add(length)
        .filter(|start| *start <= bytes.len())
        .context("truncated state header")?;
    let envelope: Envelope<T> = serde_json::from_slice(&bytes[PREFIX..start])?;
    let payload = Bytes::copy_from_slice(&bytes[start..]);
    let mut offset = 0usize;
    let mut files = Vec::with_capacity(envelope.files.len());
    for file in envelope.files {
        let end = offset
            .checked_add(usize::try_from(file.bytes)?)
            .filter(|end| *end <= payload.len())
            .context("truncated LTX data")?;
        files.push(LtxFile {
            level: file.level,
            first: file.first,
            last: file.last,
            data: payload.slice(offset..end),
        });
        offset = end;
    }
    ensure!(offset == payload.len(), "unexpected bytes after LTX data");
    Ok((envelope.value, files))
}

impl FileHeader {
    fn new(file: &LtxFile) -> Self {
        Self {
            level: file.level,
            first: file.first,
            last: file.last,
            bytes: file.data.len() as u64,
        }
    }
}
