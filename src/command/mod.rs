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
    Keys(Resp<'c>),
    Info(Option<Resp<'c>>),
    Save,
    ReplConf(Resp<'c>, Resp<'c>),
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
    pub fn parse(input: &'c [u8]) -> Result<(Self, &'c [u8]), CommandError> {
        use Command::*;
        use CommandError::*;
        let (packet, rest) = Resp::parse_inner(input)?;
        let result = match packet {
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
                    &"KEYS" => Ok(Self::Keys(
                        array
                            .get(1)
                            .and_then(|k| {
                                Some(Resp::BulkString(
                                    k.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .ok_or(IncorrectFormat)?,
                    )),
                    &"SAVE" => Ok(Self::Save),
                    &"INFO" => Ok(Self::Info(array.get(1).and_then(|parameter| {
                        Some(Resp::BulkString(
                            parameter.expect_bulk_string()?.clone().into_owned().into(),
                        ))
                    }))),
                    &"REPLCONF" => Ok(Self::ReplConf(
                        array
                            .get(1)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                        array
                            .get(1)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                    )),
                    c => Err(UnsupportedCommand(c.to_string())),
                },
                _ => Err(IncorrectFormat),
            },
            _ => Err(IncorrectFormat),
        };

        result.map(|ok| (ok, rest))
    }

    pub fn name(&self) -> String {
        match self {
            Command::Ping => "PING".to_string(),
            Command::Echo(_) => "ECHO".to_string(),
            Command::Get(_) => "GET".to_string(),
            Command::Set(_, _, _) => "SET".to_string(),
            Command::ConfigGet(_) => "CONFIG".to_string(),
            Command::Keys(_) => "KEYS".to_string(),
            Command::Info(_) => "INFO".to_string(),
            Command::Save => "SAVE".to_string(),
            Command::ReplConf(_, _) => "REPLCONF".to_string(),
        }
    }
}
