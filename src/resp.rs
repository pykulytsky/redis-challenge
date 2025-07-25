#![allow(dead_code, unused)]
use std::str::{self, from_utf8, Utf8Error};
use std::{borrow::Cow, io::Write};
use thiserror::Error;

use crate::command::Command;
use crate::config;
use crate::rdb::RdbString;

pub const CTRLF: &[u8] = b"\r\n";

#[derive(Eq, Hash, PartialEq)]
pub enum Resp<'r, S = str>
where
    S: ToOwned<Owned = String> + ?Sized,
{
    SimpleString(Cow<'r, S>),
    SimpleError(Cow<'r, S>),
    Integer(i64),
    BulkString(Cow<'r, S>),
    Array(Vec<Resp<'r, S>>),
}

#[derive(Debug, Error)]
pub enum RespError {
    #[error("Can not parse data as UTF-8")]
    UtfError(#[from] Utf8Error),

    #[error("Encountered unsuported type: {0}")]
    UnsuportedType(char),

    #[error("Can not parse data as integer")]
    NotAnInteger(#[from] std::num::ParseIntError),

    #[error("Data is not terminated with CTRLF")]
    NoCtrlf,

    #[error("There is no enough parts for the provided type")]
    NotEnoughtParts,
}

impl<'input, S> Resp<'input, S>
where
    S: ToOwned<Owned = String> + ?Sized + 'input,
{
    pub fn into_owned(self) -> Resp<'static, S> {
        match self {
            Resp::SimpleString(s) => Resp::SimpleString(Cow::Owned(s.into_owned())),
            Resp::SimpleError(e) => Resp::SimpleError(Cow::Owned(e.into_owned())),
            Resp::Integer(i) => Resp::Integer(i),
            Resp::BulkString(bs) => Resp::BulkString(Cow::Owned(bs.into_owned())),
            Resp::Array(array) => Resp::Array(array.into_iter().map(|i| i.into_owned()).collect()),
        }
    }
}

impl<'r> Resp<'r> {
    pub fn parse_inner<'i: 'r>(input: &'i [u8]) -> Result<(Self, &'i [u8]), RespError> {
        use Resp::*;
        use RespError::*;
        let len = input.len();
        let resp_value = match input[0] {
            b'+' => Ok(SimpleString(Cow::Borrowed(from_utf8(
                input.get(1..len - 2).ok_or(NotEnoughtParts)?,
            )?))),
            b'-' => Ok(SimpleError(Cow::Borrowed(from_utf8(
                input.get(1..len - 2).ok_or(NotEnoughtParts)?,
            )?))),
            b':' => Ok(Integer(
                from_utf8(
                    input
                        .get(1..input.iter().position(|b| *b == b'\r').unwrap())
                        .ok_or(NotEnoughtParts)?,
                )?
                .parse::<i64>()?,
            )),
            b'$' => {
                let mut parts = &mut input
                    .get(1..)
                    .ok_or(NotEnoughtParts)?
                    .split(|b| b == &0xA)
                    .map(|line| line.strip_suffix(&[0xD]).unwrap_or(line));
                let mut length =
                    from_utf8(parts.next().ok_or(NotEnoughtParts)?)?.parse::<isize>()?;
                if length == -1 {
                    length = 0;
                }
                let string = from_utf8(parts.next().ok_or(NotEnoughtParts)?)?;
                assert_eq!(string.len(), length as usize);
                Ok(BulkString(Cow::Borrowed(string)))
            }
            b'*' => {
                let (length_string, mut rest) =
                    input.split_at(input.iter().position(|b| b == &0xA).unwrap() + 1);
                let length = from_utf8(
                    length_string
                        .get(1..length_string.len() - 2)
                        .ok_or(NotEnoughtParts)?,
                )?
                .parse::<isize>()?;
                let mut array = vec![];
                for i in 0..length {
                    let (value, new_rest) = Self::parse_inner(rest)?;
                    array.push(value);
                    rest = new_rest;
                }
                Ok(Self::Array(array))
            }
            c => {
                let _ = dbg!(input.len());
                Err(UnsuportedType(c as char))
            }
        };

        let (_, mut rest) = input.split_at(input.iter().position(|b| b == &0xA).unwrap() + 1);
        if matches!(resp_value, Ok(BulkString(ref s))  if !s.is_empty()) {
            rest = rest
                .split_at(rest.iter().position(|b| b == &0xA).unwrap() + 1)
                .1;
        } else if matches!(resp_value, Ok(Array(_))) {
            rest = b"";
        }

        resp_value.map(|r| (r, rest))
    }

    pub fn len(&self) -> usize {
        match self {
            Resp::SimpleString(s) => s.len(),
            Resp::SimpleError(e) => e.len(),
            Resp::Integer(i) => i.checked_ilog10().unwrap_or(0) as usize + 1,
            Resp::BulkString(s) => s.len(),
            Resp::Array(vec) => vec.iter().map(|i| i.len()).sum(),
        }
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buf = vec![];
        match self {
            Resp::SimpleString(s) => {
                buf.push(b'+');
                buf.extend(s.as_bytes());
                buf.extend(CTRLF);
            }
            Resp::SimpleError(e) => {
                buf.push(b'-');
                buf.extend(e.as_bytes());
                buf.extend(CTRLF);
            }
            Resp::Integer(i) => {
                buf.push(b':');
                buf.extend(format!("{i}").as_bytes());
                buf.extend(CTRLF);
            }
            Resp::BulkString(b) => {
                buf.push(b'$');
                write!(buf, "{}", if !b.is_empty() { b.len() as isize } else { -1 });
                buf.extend(CTRLF);
                buf.extend(b.as_bytes());
                if !b.is_empty() {
                    buf.extend(CTRLF);
                }
            }
            Resp::Array(vec) => {
                buf.push(b'*');
                write!(buf, "{}", vec.len());
                buf.extend(CTRLF);
                for i in vec {
                    buf.extend(i.encode());
                }
            }
        }
        buf
    }

    pub fn parse<'i: 'r>(input: &'i [u8]) -> Result<Self, RespError> {
        let (resp, rest) = Self::parse_inner(input)?;

        Ok(resp)
    }

    pub fn simple_string(input: &'r str) -> Self {
        Self::SimpleString(Cow::Borrowed(input))
    }

    pub fn bulk_string(input: &'r str) -> Self {
        Self::BulkString(Cow::Borrowed(input))
    }

    pub fn array(input: Vec<Resp<'r>>) -> Self {
        Self::Array(input)
    }

    pub fn expect_integer(&self) -> Option<i64> {
        match self {
            Resp::Integer(i) => Some(*i),
            Resp::BulkString(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn expect_bulk_string(&self) -> Option<&Cow<'_, str>> {
        match self {
            Resp::BulkString(s) => Some(s),
            _ => None,
        }
    }

    pub fn expect_simple_string(&self) -> Option<&Cow<'_, str>> {
        match self {
            Resp::SimpleString(s) => Some(s),
            _ => None,
        }
    }
}

impl<'r> std::fmt::Debug for Resp<'r> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString(s) => write!(f, "+\"{s}\""),
            Self::SimpleError(e) => write!(f, "-\"{e}\""),
            Self::Integer(i) => write!(f, "{i}"),
            Self::BulkString(bs) => write!(f, "${} {}", bs.len(), bs),
            Self::Array(array) => {
                write!(f, "[")?;
                write!(f, "{:?}", array[0]);
                for i in array.iter().skip(1) {
                    write!(f, ", {:?}", i)?;
                }
                write!(f, "]")
            }
        }
    }
}

impl<'input, S> Clone for Resp<'input, S>
where
    S: ToOwned<Owned = String> + ?Sized + 'input,
{
    fn clone(&self) -> Self {
        match self {
            Resp::SimpleString(cow) => Resp::SimpleString(cow.clone()),
            Resp::SimpleError(cow) => Resp::SimpleString(cow.clone()),
            Resp::Integer(i) => Resp::Integer(*i),
            Resp::BulkString(cow) => Resp::BulkString(cow.clone()),
            Resp::Array(vec) => Resp::Array(vec.clone()),
        }
    }
}

impl<'input> From<RdbString> for Resp<'input> {
    fn from(value: RdbString) -> Self {
        Self::SimpleString(Cow::Owned(value.0))
    }
}

impl<'c> From<Command<'c>> for Resp<'c> {
    fn from(command: Command<'c>) -> Self {
        let mut array = vec![Resp::BulkString(Cow::Owned(command.name()))];
        match command {
            Command::Ping => {}
            Command::Echo(msg) => {
                array.push(Resp::BulkString(Cow::Owned(msg)));
            }
            Command::Get(key) => {
                array.push(key);
            }
            Command::Set(key, value, expiry) => {
                array.push(key);
                array.push(value);
                if let Some(exp) = expiry {
                    array.push(Resp::bulk_string("EX"));
                    array.push(Resp::Integer(exp))
                }
            }
            Command::ConfigGet(config_item) => {
                array.push(Resp::BulkString(Cow::Owned(format!("{:?}", config_item))))
            }
            Command::Keys(resp) => {
                array.push(resp);
            }
            Command::Info(resp) => {
                if let Some(info) = resp {
                    array.push(info);
                }
            }
            Command::Save => {}
            Command::ReplConf(key, value) => {
                array.push(key);
                array.push(value);
            }
        }

        Resp::Array(array)
    }
}
