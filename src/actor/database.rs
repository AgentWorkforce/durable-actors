use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json::{
    Value,
    value::{RawValue, to_raw_value},
};
use std::collections::BTreeMap;

#[async_trait]
pub trait ActorDatabase: Send + Sync {
    async fn call(&self, call: DatabaseCall) -> Result<DatabaseResult>;
}

#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DatabaseCall {
    Exec {
        sql: String,
        #[serde(default)]
        bindings: Vec<SqlValue>,
        #[serde(default)]
        immediate: bool,
    },
    /// Every stored field, or only `names` when given.
    Fields {
        #[serde(skip_serializing_if = "Option::is_none")]
        names: Option<Vec<String>>,
    },
    InitializeFields {
        fields: BTreeMap<String, Box<RawValue>>,
    },
    PersistField {
        name: String,
        value: Box<RawValue>,
        #[serde(default)]
        immediate: bool,
    },
    DeleteField {
        name: String,
        #[serde(default)]
        immediate: bool,
    },
    ReplaceFields {
        fields: BTreeMap<String, Box<RawValue>>,
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

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum DatabaseCallWire {
    Exec {
        sql: String,
        #[serde(default)]
        bindings: Vec<SqlValue>,
        #[serde(default)]
        immediate: bool,
    },
    Fields {
        #[serde(default)]
        names: Option<Vec<String>>,
    },
    InitializeFields {
        fields: BTreeMap<String, Value>,
    },
    PersistField {
        name: String,
        value: Value,
        #[serde(default)]
        immediate: bool,
    },
    DeleteField {
        name: String,
        #[serde(default)]
        immediate: bool,
    },
    ReplaceFields {
        fields: BTreeMap<String, Value>,
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

impl<'de> Deserialize<'de> for DatabaseCall {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match DatabaseCallWire::deserialize(deserializer)? {
            DatabaseCallWire::Exec {
                sql,
                bindings,
                immediate,
            } => Self::Exec {
                sql,
                bindings,
                immediate,
            },
            DatabaseCallWire::Fields { names } => Self::Fields { names },
            DatabaseCallWire::InitializeFields { fields } => Self::InitializeFields {
                fields: raw_fields(fields)?,
            },
            DatabaseCallWire::PersistField {
                name,
                value,
                immediate,
            } => Self::PersistField {
                name,
                value: to_raw_value(&value).map_err(D::Error::custom)?,
                immediate,
            },
            DatabaseCallWire::DeleteField { name, immediate } => {
                Self::DeleteField { name, immediate }
            }
            DatabaseCallWire::ReplaceFields { fields } => Self::ReplaceFields {
                fields: raw_fields(fields)?,
            },
            DatabaseCallWire::Alarm => Self::Alarm,
            DatabaseCallWire::SetAlarm {
                timestamp_ms,
                immediate,
            } => Self::SetAlarm {
                timestamp_ms,
                immediate,
            },
            DatabaseCallWire::DeleteAlarm { immediate } => Self::DeleteAlarm { immediate },
            DatabaseCallWire::Commit => Self::Commit,
            DatabaseCallWire::Rollback => Self::Rollback,
        })
    }
}

fn raw_fields<E: Error>(
    fields: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Box<RawValue>>, E> {
    fields
        .into_iter()
        .map(|(name, value)| Ok((name, to_raw_value(&value).map_err(E::custom)?)))
        .collect()
}

impl DatabaseCall {
    pub(crate) fn immediate(&self) -> bool {
        matches!(
            self,
            Self::Exec {
                immediate: true,
                ..
            } | Self::PersistField {
                immediate: true,
                ..
            } | Self::DeleteField {
                immediate: true,
                ..
            } | Self::SetAlarm {
                immediate: true,
                ..
            } | Self::DeleteAlarm {
                immediate: true,
                ..
            }
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatabaseResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    },
    Fields {
        fields: BTreeMap<String, Box<RawValue>>,
    },
    Alarm {
        timestamp_ms: Option<i64>,
    },
    Done,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SqlValue {
    Null,
    Integer(#[serde(with = "integer")] i64),
    Real(f64),
    Text(String),
    Blob(#[serde(with = "blob")] Vec<u8>),
}

mod integer {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(super) fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

mod blob {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(super) fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(value))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        STANDARD
            .decode(String::deserialize(deserializer)?)
            .map_err(D::Error::custom)
    }
}
