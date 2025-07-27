use std::collections::HashMap;

use indexmap::IndexMap;

use crate::resp::Resp;

#[derive(Debug, Clone)]
pub enum Value {
    Str(String),
    Array(Vec<Value>),
    Stream(IndexMap<String, IndexMap<String, Value>>),
}

impl Value {
    pub fn expect_string(self) -> Option<String> {
        match self {
            Value::Str(str) => Some(str),
            _ => None,
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
            Resp::Array(resps) => Self::Array(
                resps
                    .into_iter()
                    .map(|resp| From::<Resp<'_>>::from(resp))
                    .collect(),
            ),
        }
    }
}
