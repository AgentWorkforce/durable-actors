use super::*;
use aws_lc_rs::digest::{Context as Digest, SHA256};
use std::io::Write;

pub(super) const HEADER: usize = 48;

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub version: u64,
    pub state: Payload,
}

/// A record as it is logged: its header, checksum included, then its state.
#[derive(Clone)]
pub struct Frame {
    pub header: Bytes,
    pub state: Payload,
}

impl Record {
    pub fn frame(self) -> Result<Frame> {
        ensure!(self.version > 0, "invalid state version");
        let length = u32::try_from(self.state.len()).context("record length overflow")?;
        let mut header = Vec::with_capacity(HEADER);
        header.extend_from_slice(b"RLG1");
        header.extend_from_slice(&length.to_le_bytes());
        header.extend_from_slice(&self.version.to_le_bytes());
        header.extend_from_slice(checksum(&header, &self.state)?.as_ref());
        Ok(Frame {
            header: header.into(),
            state: self.state,
        })
    }

    pub fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        self.clone().frame()?.write_to(writer)
    }
}

impl Frame {
    pub fn len(&self) -> usize {
        self.header.len() + self.state.len()
    }

    pub fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&self.header)?;
        self.state.write_to(writer)?;
        Ok(())
    }
}

pub(super) fn decode(bytes: &Payload) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut offset = 0;
    while bytes.len() - offset >= HEADER {
        let header = bytes.read(offset..offset + HEADER)?;
        let end = offset
            .checked_add(length(&header)?)
            .context("record offset overflow")?;
        if end > bytes.len() {
            break;
        }
        let version = u64::from_le_bytes(header[8..16].try_into()?);
        ensure!(version > 0, "invalid state version");
        if let Some(previous) = records.last() {
            let previous: &Record = previous;
            ensure!(
                previous.version.checked_add(1) == Some(version),
                "nonconsecutive state versions"
            );
        }
        let state = bytes.slice(offset + HEADER..end);
        ensure!(
            checksum(&header[..16], &state)?.as_ref() == &header[16..48],
            "corrupt log record"
        );
        records.push(Record { version, state });
        offset = end;
    }
    Ok(records)
}

pub(super) fn length(header: &[u8]) -> Result<usize> {
    ensure!(
        header.len() >= HEADER && &header[..4] == b"RLG1",
        "invalid record header"
    );
    HEADER
        .checked_add(u32::from_le_bytes(header[4..8].try_into()?) as usize)
        .context("record length overflow")
}

fn checksum(header: &[u8], state: &Payload) -> std::io::Result<aws_lc_rs::digest::Digest> {
    let mut hash = Digest::new(&SHA256);
    hash.update(header);
    state.for_each_chunk(|chunk| {
        hash.update(chunk);
        Ok(())
    })?;
    Ok(hash.finish())
}
