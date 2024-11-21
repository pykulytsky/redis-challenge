#![allow(dead_code, unused)]
use std::str::{self, from_utf8, Utf8Error};
use thiserror::Error;

pub const CTRLF: &[u8] = b"\r\n";

pub enum Resp<'r> {
    SimpleString(&'r str),
    SimpleError(&'r str),
    Integer(i64),
    BulkString(&'r str),
    Array(Vec<Resp<'r>>),
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

impl<'r> Resp<'r> {
    pub fn parse_inner<'i: 'r>(input: &'i [u8]) -> Result<(Self, &'i [u8]), RespError> {
        use Resp::*;
        use RespError::*;
        let len = input.len();
        let resp_value = match input[0] {
            b'+' => Ok(SimpleString(from_utf8(&input[1..len - 2])?)),
            b'-' => Ok(SimpleError(from_utf8(&input[1..len - 2])?)),
            b':' => Ok(Integer(from_utf8(&input[1..len - 2])?.parse::<i64>()?)),
            b'$' => {
                let mut parts = &mut input[1..]
                    .split(|b| b == &0xA)
                    .map(|line| line.strip_suffix(&[0xD]).unwrap_or(line));
                let mut length =
                    from_utf8(parts.next().ok_or(NotEnoughtParts)?)?.parse::<isize>()?;
                if length == -1 {
                    length = 0;
                }
                let string = from_utf8(parts.next().ok_or(NotEnoughtParts)?)?;
                assert_eq!(string.len(), length as usize);
                Ok(BulkString(string))
            }
            b'*' => {
                let (length_string, mut rest) =
                    input.split_at(input.iter().position(|b| b == &0xA).unwrap() + 1);
                let length =
                    from_utf8(&length_string[1..length_string.len() - 2])?.parse::<isize>()?;
                let mut array = vec![];
                for i in 0..length {
                    let (value, new_rest) = Self::parse_inner(rest)?;
                    array.push(value);
                    rest = new_rest;
                }
                assert!(rest.is_empty());
                Ok(Self::Array(array))
            }
            c => Err(UnsuportedType(c as char)),
        };

        let (_, mut rest) = input.split_at(input.iter().position(|b| b == &0xA).unwrap() + 1);
        if matches!(resp_value, Ok(BulkString(s))  if !s.is_empty()) {
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
                buf.push(b.len() as u8);
                buf.extend(CTRLF);
                buf.extend(b.as_bytes());
                buf.extend(CTRLF);
            }
            Resp::Array(vec) => {
                buf.push(b'*');
                buf.push(vec.len() as u8);
                for i in vec {
                    buf.extend(i.encode());
                }
            }
        }
        buf
    }

    pub fn parse<'i: 'r>(input: &'i [u8]) -> Result<Self, RespError> {
        let (resp, rest) = Self::parse_inner(input)?;
        assert!(rest.is_empty());

        Ok(resp)
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
