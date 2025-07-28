use std::collections::HashMap;

use indexmap::IndexMap;

use crate::{rdb::RdbString, resp::Resp};

#[derive(Debug, Clone)]
pub enum Value {
    Str(String),
    List(Vec<Value>),
    Stream(IndexMap<String, IndexMap<String, Value>>),
}

impl Value {
    pub fn expect_string(self) -> Option<String> {
        match self {
            Value::Str(str) => Some(str),
            _ => None,
        }
    }

    pub fn value_type(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Stream(_) => "stream",
        }
    }
}

impl From<Resp<'_>> for Value {
    fn from(resp: Resp<'_>) -> Self {
        match resp {
            Resp::SimpleString(cow) => Self::Str(cow.into_owned()),
            Resp::SimpleError(cow) => Self::Str(cow.into_owned()),
            Resp::Integer(number) => Self::Str(number.to_string()),
            Resp::BulkString(cow) => Self::Str(cow.into_owned()),
            Resp::Array(resps) => Self::List(
                resps
                    .into_iter()
                    .map(|resp| From::<Resp<'_>>::from(resp))
                    .collect(),
            ),
        }
    }
}

impl From<RdbString> for Value {
    fn from(value: RdbString) -> Self {
        Self::Str(value.0)
    }
}
