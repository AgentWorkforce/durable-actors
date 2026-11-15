use crate::actor::{RowSink, SqlCell, SqlValue};
use anyhow::Result;
use std::sync::{Arc, Mutex};

/// Keeps what a statement writes, for a test to read once the call returns.
#[derive(Clone, Default)]
pub struct Rows(Arc<Mutex<Collected>>);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Collected {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub finished: bool,
}

impl Rows {
    pub fn sink(&self) -> Box<dyn RowSink> {
        Box::new(self.clone())
    }

    pub fn collected(&self) -> Collected {
        self.0.lock().unwrap().clone()
    }
}

impl Collected {
    /// What a field read wrote: each row is a field's name and its JSON.
    pub fn fields(&self) -> Result<serde_json::Value> {
        let fields = self.rows.iter().map(|row| match row.as_slice() {
            [SqlValue::Text(name), SqlValue::Text(json)] => {
                Ok((name.clone(), serde_json::from_str(json)?))
            }
            row => anyhow::bail!("a field is its name and its JSON, not {row:?}"),
        });
        Ok(serde_json::Value::Object(fields.collect::<Result<_>>()?))
    }
}

pub fn ignored() -> Box<dyn RowSink> {
    Rows::default().sink()
}

impl RowSink for Rows {
    fn columns(&mut self, names: &[&str]) -> Result<()> {
        self.0.lock().unwrap().columns = names.iter().map(|name| name.to_string()).collect();
        Ok(())
    }

    fn row(&mut self, cells: &[SqlCell<'_>]) -> Result<()> {
        let row = cells.iter().map(owned).collect();
        self.0.lock().unwrap().rows.push(row);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.0.lock().unwrap().finished = true;
        Ok(())
    }
}

fn owned(cell: &SqlCell<'_>) -> SqlValue {
    match *cell {
        SqlCell::Null => SqlValue::Null,
        SqlCell::Integer(value) => SqlValue::Integer(value),
        SqlCell::Real(value) => SqlValue::Real(value),
        SqlCell::Text(value) => SqlValue::Text(String::from_utf8_lossy(value).into_owned()),
        SqlCell::Blob(value) => SqlValue::Blob(value.to_vec()),
    }
}
