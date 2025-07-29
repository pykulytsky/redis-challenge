#![allow(dead_code)]

use crate::{command::CommandError, resp::Resp};

#[derive(Debug, Clone, PartialEq)]
pub struct Get {
    pub key: String,
}

impl Get {
    pub fn parse(input: &[Resp<'_>]) -> Result<Self, CommandError> {
        let key = input
            .get(1)
            .and_then(|k| k.expect_bulk_string())
            .ok_or(CommandError::IncorrectFormat)?
            .to_string();
        Ok(Self { key })
    }
}
