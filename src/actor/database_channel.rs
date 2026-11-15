//! The connection an executor makes its database calls on, apart from its commands so that the
//! actor's thread can block on it.
//!
//! One call is answered at a time. A request is a `u32` size, that much JSON, then the values
//! the JSON counts under `bindings`: what a statement binds, or the JSON of each field the call
//! writes. The answer is a stream of records: `COLUMNS` and a `ROW` per row while a statement or
//! a field read runs, then `RESULT` or `FAILED`. Integers are little-endian, and a value is a
//! tag followed by its eight bytes, or by a `u32` size and that many bytes.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
};

use super::{ActorDatabase, DatabaseCall, DatabaseResult, RowSink, SqlCell, SqlValue};

const NULL: u8 = 0;
const INTEGER: u8 = 1;
const REAL: u8 = 2;
const TEXT: u8 = 3;
const BLOB: u8 = 4;

const COLUMNS: u8 = 0x10;
const ROW: u8 = 0x11;
const RESULT: u8 = 0x12;
const FAILED: u8 = 0x13;

const CHUNK_BYTES: usize = 64 * 1024;
const CHUNKS_IN_FLIGHT: usize = 4;
/// SQLite's own limit on the values one statement binds.
const MAX_BINDINGS: usize = 32_766;

/// The database each running command reaches, and the activation's for calls that name none.
#[derive(Default)]
pub(super) struct DatabaseScopes(Mutex<Scopes>);

#[derive(Default)]
struct Scopes {
    commands: HashMap<u64, Arc<dyn ActorDatabase>>,
    activation: Option<Arc<dyn ActorDatabase>>,
}

impl DatabaseScopes {
    pub(super) fn activate(&self, database: Option<Arc<dyn ActorDatabase>>) {
        self.0.lock().unwrap().activation = database;
    }

    pub(super) fn open(&self, message_id: u64, database: Arc<dyn ActorDatabase>) {
        self.0.lock().unwrap().commands.insert(message_id, database);
    }

    pub(super) fn close(&self, message_id: u64) {
        self.0.lock().unwrap().commands.remove(&message_id);
    }

    /// A call that names a command is refused once that command has been answered: it may come
    /// from an executor that was stopped while the command ran.
    fn resolve(&self, message_id: Option<u64>) -> Result<Arc<dyn ActorDatabase>> {
        let scopes = self.0.lock().unwrap();
        match message_id {
            Some(id) => scopes
                .commands
                .get(&id)
                .cloned()
                .context("the command this database call belongs to has ended"),
            None => scopes
                .activation
                .clone()
                .context("actor database is unavailable"),
        }
    }
}

pub(super) async fn serve(stream: UnixStream, scopes: Arc<DatabaseScopes>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::with_capacity(CHUNK_BYTES, reader);
    while let Some(request) = read_request(&mut reader).await? {
        answer(&mut writer, request, &scopes).await?;
    }
    Ok(())
}

struct Request {
    message_id: Option<u64>,
    call: DatabaseCall,
}

#[derive(Deserialize)]
struct RequestHead {
    #[serde(default)]
    message_id: Option<u64>,
    call: CallHead,
    #[serde(default)]
    bindings: usize,
}

/// A call without the values it carries. They follow the head, so a large one is never escaped
/// into the head's JSON or parsed out of it.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CallHead {
    Exec {
        sql: String,
        #[serde(default)]
        immediate: bool,
    },
    Fields {
        #[serde(default)]
        names: Option<Vec<String>>,
    },
    InitializeFields {
        names: Vec<String>,
    },
    PersistField {
        name: String,
        #[serde(default)]
        immediate: bool,
    },
    DeleteField {
        name: String,
        #[serde(default)]
        immediate: bool,
    },
    Alarm,
    SetAlarm {
        timestamp_ms: i64,
        #[serde(default)]
        immediate: bool,
    },
    DeleteAlarm {
        #[serde(default)]
        immediate: bool,
    },
    Commit,
    Rollback,
}

impl CallHead {
    /// How many values follow the head. Only a statement leaves that to the request.
    fn values(&self) -> Option<usize> {
        match self {
            Self::Exec { .. } => None,
            Self::InitializeFields { names } => Some(names.len()),
            Self::PersistField { .. } => Some(1),
            _ => Some(0),
        }
    }
}

type Reader = BufReader<OwnedReadHalf>;

async fn read_request(reader: &mut Reader) -> Result<Option<Request>> {
    if reader.fill_buf().await?.is_empty() {
        return Ok(None);
    }
    let head: RequestHead =
        serde_json::from_slice(&read_sized(reader).await?).context("decode database call")?;
    ensure!(
        head.bindings <= MAX_BINDINGS,
        "database call binds too many values"
    );
    ensure!(
        head.call
            .values()
            .is_none_or(|values| values == head.bindings),
        "database call carries the wrong number of values"
    );
    Ok(Some(Request {
        message_id: head.message_id,
        call: read_call(reader, head.call, head.bindings).await?,
    }))
}

async fn read_call(reader: &mut Reader, head: CallHead, bindings: usize) -> Result<DatabaseCall> {
    Ok(match head {
        CallHead::Exec { sql, immediate } => DatabaseCall::Exec {
            sql,
            bindings: read_bindings(reader, bindings).await?,
            immediate,
        },
        CallHead::Fields { names } => DatabaseCall::Fields { names },
        CallHead::InitializeFields { names } => DatabaseCall::InitializeFields {
            fields: read_fields(reader, names).await?,
        },
        CallHead::PersistField { name, immediate } => DatabaseCall::PersistField {
            name,
            value: read_json(reader).await?,
            immediate,
        },
        CallHead::DeleteField { name, immediate } => DatabaseCall::DeleteField { name, immediate },
        CallHead::Alarm => DatabaseCall::Alarm,
        CallHead::SetAlarm {
            timestamp_ms,
            immediate,
        } => DatabaseCall::SetAlarm {
            timestamp_ms,
            immediate,
        },
        CallHead::DeleteAlarm { immediate } => DatabaseCall::DeleteAlarm { immediate },
        CallHead::Commit => DatabaseCall::Commit,
        CallHead::Rollback => DatabaseCall::Rollback,
    })
}

async fn read_bindings(reader: &mut Reader, count: usize) -> Result<Vec<SqlValue>> {
    let mut bindings = Vec::with_capacity(count);
    for _ in 0..count {
        bindings.push(read_value(reader).await?);
    }
    Ok(bindings)
}

async fn read_fields(
    reader: &mut Reader,
    names: Vec<String>,
) -> Result<BTreeMap<String, Box<RawValue>>> {
    let mut fields = BTreeMap::new();
    for name in names {
        fields.insert(name, read_json(reader).await?);
    }
    Ok(fields)
}

/// A field's JSON stays the text the executor wrote: it is checked, and never read into a tree.
async fn read_json(reader: &mut Reader) -> Result<Box<RawValue>> {
    ensure!(
        reader.read_u8().await? == TEXT,
        "a field's value is sent as text"
    );
    let text = String::from_utf8(read_sized(reader).await?).context("field value is not UTF-8")?;
    RawValue::from_string(text).context("field value is not JSON")
}

async fn read_value(reader: &mut Reader) -> Result<SqlValue> {
    Ok(match reader.read_u8().await? {
        NULL => SqlValue::Null,
        INTEGER => SqlValue::Integer(reader.read_i64_le().await?),
        REAL => SqlValue::Real(reader.read_f64_le().await?),
        TEXT => SqlValue::Text(String::from_utf8(read_sized(reader).await?)?),
        BLOB => SqlValue::Blob(read_sized(reader).await?),
        tag => anyhow::bail!("unknown database value type {tag}"),
    })
}

// The executor chooses the size, so memory is claimed only as its bytes arrive.
async fn read_sized(reader: &mut Reader) -> Result<Vec<u8>> {
    let size = reader.read_u32_le().await? as usize;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .context("database call is too large")?;
    reader.take(size as u64).read_to_end(&mut bytes).await?;
    ensure!(
        bytes.len() == size,
        "executor disconnected inside a database call"
    );
    Ok(bytes)
}

async fn answer(
    writer: &mut OwnedWriteHalf,
    request: Request,
    scopes: &DatabaseScopes,
) -> Result<()> {
    let (rows, mut chunks) = mpsc::channel(CHUNKS_IN_FLIGHT);
    let (result, forwarded) = tokio::join!(
        call(request, scopes, RowStream::new(rows)),
        forward(&mut chunks, writer)
    );
    forwarded?;
    writer.write_all(&ending(result)?).await?;
    Ok(())
}

async fn call(
    request: Request,
    scopes: &DatabaseScopes,
    rows: RowStream,
) -> Result<DatabaseResult> {
    scopes
        .resolve(request.message_id)?
        .call(request.call, Box::new(rows))
        .await
}

/// Ends once the call has dropped its sink. Closing after a failed write releases a statement
/// that is waiting to send.
async fn forward(chunks: &mut mpsc::Receiver<Vec<u8>>, writer: &mut OwnedWriteHalf) -> Result<()> {
    while let Some(chunk) = chunks.recv().await {
        if let Err(error) = writer.write_all(&chunk).await {
            chunks.close();
            return Err(error.into());
        }
    }
    Ok(())
}

fn ending(result: Result<DatabaseResult>) -> Result<Vec<u8>> {
    let (kind, body) = match result {
        Ok(result) => (RESULT, serde_json::to_vec(&result)?),
        Err(error) => (FAILED, format!("{error:#}").into_bytes()),
    };
    let mut record = vec![kind];
    record.extend(u32::try_from(body.len())?.to_le_bytes());
    record.extend(body);
    Ok(record)
}

/// Sends rows in chunks of a fixed size, however large a row or a value is, and waits when the
/// executor reads slower than SQLite produces.
struct RowStream {
    chunks: mpsc::Sender<Vec<u8>>,
    chunk: Vec<u8>,
}

impl RowStream {
    fn new(chunks: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            chunks,
            chunk: Vec::new(),
        }
    }

    fn value(&mut self, cell: &SqlCell<'_>) -> Result<()> {
        match *cell {
            SqlCell::Null => self.put(&[NULL]),
            SqlCell::Integer(value) => {
                self.put(&[INTEGER])?;
                self.put(&value.to_le_bytes())
            }
            SqlCell::Real(value) => {
                self.put(&[REAL])?;
                self.put(&value.to_le_bytes())
            }
            SqlCell::Text(value) => {
                self.put(&[TEXT])?;
                self.sized(value)
            }
            SqlCell::Blob(value) => {
                self.put(&[BLOB])?;
                self.sized(value)
            }
        }
    }

    fn sized(&mut self, bytes: &[u8]) -> Result<()> {
        self.put(&u32::try_from(bytes.len())?.to_le_bytes())?;
        self.put(bytes)
    }

    fn put(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let room = CHUNK_BYTES - self.chunk.len();
            let (fitting, rest) = bytes.split_at(bytes.len().min(room));
            self.chunk.extend_from_slice(fitting);
            bytes = rest;
            if self.chunk.len() == CHUNK_BYTES {
                self.send()?;
            }
        }
        Ok(())
    }

    fn send(&mut self) -> Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.chunk, Vec::with_capacity(CHUNK_BYTES));
        self.chunks
            .blocking_send(chunk)
            .map_err(|_| anyhow::anyhow!("executor stopped reading its database result"))
    }
}

impl RowSink for RowStream {
    fn columns(&mut self, names: &[&str]) -> Result<()> {
        self.put(&[COLUMNS])?;
        self.put(&u32::try_from(names.len())?.to_le_bytes())?;
        names
            .iter()
            .try_for_each(|name| self.sized(name.as_bytes()))
    }

    fn row(&mut self, cells: &[SqlCell<'_>]) -> Result<()> {
        self.put(&[ROW])?;
        cells.iter().try_for_each(|cell| self.value(cell))
    }

    fn finish(&mut self) -> Result<()> {
        self.send()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/actor/database_channel.rs"]
mod tests;
