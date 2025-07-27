use std::borrow::Cow;

use crate::resp::{Resp, RespError};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigItem {
    Dir,
    DbFileName,
}

#[derive(Debug, Clone, PartialEq)]
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
    Psync(Resp<'c>, Resp<'c>),
    Wait(Resp<'c>, Resp<'c>),
    Select(Resp<'c>),
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
    pub fn is_write_command(&self) -> bool {
        match self {
            Command::Set(_, _, _) => true,
            _ => false,
        }
    }

    pub fn should_account(&self) -> bool {
        match self {
            Command::Set(_, _, _) => true,
            Command::Ping => true,
            Command::ReplConf(_, _) => true,
            // Command::ReplConf(key, _) => {
            //     // GETACK commands should not be counted towards replica offset
            //     match key {
            //         crate::resp::Resp::BulkString(cow) => cow.as_ref() != "GETACK",
            //         _ => true,
            //     }
            // }
            _ => false,
        }
    }

    pub fn into_owned(self) -> Command<'static> {
        match self {
            Command::Ping => Command::Ping,
            Command::Echo(msg) => Command::Echo(msg),
            Command::Get(resp) => Command::Get(resp.into_owned()),
            Command::Set(resp, resp1, resp2) => {
                Command::Set(resp.into_owned(), resp1.into_owned(), resp2)
            }
            Command::ConfigGet(config_item) => Command::ConfigGet(config_item),
            Command::Keys(resp) => Command::Keys(resp.into_owned()),
            Command::Info(resp) => Command::Info(resp.map(|resp| resp.into_owned())),
            Command::Save => Command::Save,
            Command::ReplConf(resp, resp1) => {
                Command::ReplConf(resp.into_owned(), resp1.into_owned())
            }
            Command::Psync(resp, resp1) => Command::Psync(resp.into_owned(), resp1.into_owned()),
            Command::Wait(resp, resp1) => Command::Wait(resp.into_owned(), resp1.into_owned()),
            Command::Select(resp) => Command::Select(resp.into_owned()),
        }
    }

    pub fn parse(input: &'c [u8]) -> Result<(Self, &'c [u8]), CommandError> {
        use Command::*;
        use CommandError::*;
        let (packet, rest) = Resp::parse_inner(input)?;
        let result = match packet {
            Resp::Array(array) => match array.first().ok_or(IncorrectFormat)? {
                Resp::BulkString(Cow::Borrowed(c)) => match &c.to_uppercase().as_str() {
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
                            .get(2)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                    )),
                    &"PSYNC" => Ok(Self::Psync(
                        array
                            .get(1)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                        array
                            .get(2)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                    )),
                    &"WAIT" => Ok(Self::Wait(
                        array
                            .get(1)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                        array
                            .get(2)
                            .and_then(|parameter| {
                                Some(Resp::BulkString(
                                    parameter.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .unwrap(),
                    )),
                    &"SELECT" => Ok(Self::Select(
                        array
                            .get(1)
                            .and_then(|k| {
                                Some(Resp::BulkString(
                                    k.expect_bulk_string()?.clone().into_owned().into(),
                                ))
                            })
                            .ok_or(IncorrectFormat)?,
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
            Command::Psync(_, _) => "PSYNC".to_string(),
            Command::Wait(_, _) => "WAIT".to_string(),
            Command::Select(_) => "SELECT".to_string(),
        }
    }
}
