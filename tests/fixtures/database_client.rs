//! Speaks the database channel as an SDK does.

use crate::actor::SqlValue;
use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, PartialEq)]
pub struct Answer {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub ending: Result<Value, String>,
}

pub async fn call<S>(stream: &mut S, head: Value, bindings: &[SqlValue]) -> Result<Answer>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(&request(head, bindings)?).await?;
    read_answer(stream).await
}

pub fn request(mut head: Value, bindings: &[SqlValue]) -> Result<Vec<u8>> {
    if !bindings.is_empty() {
        head["bindings"] = bindings.len().into();
    }
    let mut bytes = Vec::new();
    sized(&mut bytes, head.to_string().as_bytes());
    for binding in bindings {
        match binding {
            SqlValue::Null => bytes.push(0),
            SqlValue::Integer(value) => {
                bytes.push(1);
                bytes.extend(value.to_le_bytes());
            }
            SqlValue::Real(value) => {
                bytes.push(2);
                bytes.extend(value.to_le_bytes());
            }
            SqlValue::Text(value) => {
                bytes.push(3);
                sized(&mut bytes, value.as_bytes());
            }
            SqlValue::Blob(value) => {
                bytes.push(4);
                sized(&mut bytes, value);
            }
        }
    }
    Ok(bytes)
}

fn sized(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend((value.len() as u32).to_le_bytes());
    bytes.extend(value);
}

pub async fn read_answer<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Answer> {
    let (mut columns, mut rows) = (Vec::new(), Vec::new());
    loop {
        match stream.read_u8().await? {
            0x10 => {
                for _ in 0..stream.read_u32_le().await? {
                    columns.push(String::from_utf8(read_sized(stream).await?)?);
                }
            }
            0x11 => {
                let mut row = Vec::new();
                for _ in 0..columns.len() {
                    row.push(read_value(stream).await?);
                }
                rows.push(row);
            }
            0x12 => {
                let result = serde_json::from_slice(&read_sized(stream).await?)?;
                return Ok(Answer {
                    columns,
                    rows,
                    ending: Ok(result),
                });
            }
            0x13 => {
                let message = String::from_utf8(read_sized(stream).await?)?;
                return Ok(Answer {
                    columns,
                    rows,
                    ending: Err(message),
                });
            }
            record => bail!("unknown database record {record}"),
        }
    }
}

async fn read_value<S: AsyncRead + Unpin>(stream: &mut S) -> Result<SqlValue> {
    Ok(match stream.read_u8().await? {
        0 => SqlValue::Null,
        1 => SqlValue::Integer(stream.read_i64_le().await?),
        2 => SqlValue::Real(stream.read_f64_le().await?),
        3 => SqlValue::Text(String::from_utf8(read_sized(stream).await?)?),
        4 => SqlValue::Blob(read_sized(stream).await?),
        kind => bail!("unknown database value type {kind}"),
    })
}

async fn read_sized<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    let mut bytes = vec![0; stream.read_u32_le().await? as usize];
    stream
        .read_exact(&mut bytes)
        .await
        .context("database record ended early")?;
    Ok(bytes)
}
