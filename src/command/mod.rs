use std::borrow::Cow;

use crate::resp::{Resp, RespError};
use thiserror::Error;

#[derive(Debug)]
pub enum ConfigItem {
    Dir,
    DbFileName,
}

#[derive(Debug)]
pub enum Command<'c> {
    Ping,
    Echo(String),
    Get(Resp<'c>),
    Set(Resp<'c>, Resp<'c>, Option<i64>),
    ConfigGet(ConfigItem),
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Protocol parsing error")]
    ProtocolError(#[from] RespError),

    #[error("Unsupported command: {0}")]
    UnsupportedCommand(String),

    #[error("Incorrect command format")]
    IncorrectFormat,
}

impl<'c> Command<'c> {
    pub fn parse(input: &'c [u8]) -> Result<Self, CommandError> {
        use Command::*;
        use CommandError::*;
        let packet = Resp::parse(input)?;
        match packet {
            Resp::Array(array) => match array.first().ok_or(IncorrectFormat)? {
                Resp::BulkString(Cow::Borrowed(c)) => match c {
                    &"PING" => Ok(Ping),
                    &"ECHO" => {
                        let arg = array.get(1).ok_or(IncorrectFormat)?;
                        match arg {
                            Resp::BulkString(s) => Ok(Echo(s.to_string())),
                            _ => Err(IncorrectFormat),
                        }
                    }
                    &"GET" => {
                        let key = array.get(1).ok_or(IncorrectFormat)?;
                        Ok(Self::Get(key.clone()))
                    }

                    &"SET" => {
                        let key = array.get(1).ok_or(IncorrectFormat)?;
                        let value = array.get(2).ok_or(IncorrectFormat)?;
                        assert!(matches!(array.get(3), Some(Resp::BulkString(_)) | None));
                        let expiry = array.get(4).and_then(|e| e.expect_integer());
                        Ok(Self::Set(key.clone(), value.clone(), expiry))
                    }
                    &"CONFIG" => match array.get(1).ok_or(IncorrectFormat)? {
                        Resp::BulkString(Cow::Borrowed("GET")) => {
                            match array.get(2).ok_or(IncorrectFormat)? {
                                Resp::BulkString(Cow::Borrowed("dir")) => {
                                    Ok(Self::ConfigGet(ConfigItem::Dir))
                                }
                                Resp::BulkString(Cow::Borrowed("dbfilename")) => {
                                    Ok(Self::ConfigGet(ConfigItem::DbFileName))
                                }
                                _ => Err(IncorrectFormat),
                            }
                        }
                        _ => todo!(),
                    },
                    c => Err(UnsupportedCommand(c.to_string())),
                },
                _ => Err(IncorrectFormat),
            },
            _ => Err(IncorrectFormat),
        }
    }
}
