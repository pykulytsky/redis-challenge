#![allow(dead_code, unused)]

use crate::{config::Config, resp::RespError, InnerDb, InnerExpiries, Resp};
use core::str;
use std::{
    collections::HashMap,
    io::Write,
    path::PathBuf,
    str::{from_utf8, FromStr},
    sync::Arc,
};

use thiserror::Error;
use tokio::{io::AsyncReadExt, sync::RwLock};

use crate::{Db, Expiries};

pub const METADATA_START: u8 = 0xFA;
pub const SELECTDB: u8 = 0xFE;
pub const REDIS_VER: &str = "redis-ver";
pub const REDIS_VER_VALUE: &str = "6.0.16";
pub const START_DB_SECTION: u8 = 0xFE;
pub const DB_SIZE_FLAG: u8 = 0xFB;
pub const HAS_EXPIRY_FLAG: u8 = 0xFC;
pub const METADATA_LEN: usize = 18;
const METADATA_OFFSET: usize = 9;

#[derive(Debug, Error)]
pub enum RdbError {
    #[error("Header parse error")]
    RdbHeaderParserError,

    #[error("Config error")]
    RdbConfigError,

    #[error("metadata parse error")]
    RdbMetadataParserError,

    #[error("database parse error")]
    RdbDatabaseParserError,

    #[error("Can not parse data as UTF-8")]
    Utf8ParserError(#[from] std::str::Utf8Error),

    #[error("Can not parse data as integer")]
    NotANumber(#[from] std::num::ParseIntError),

    #[error("Error while parsing data as RESP")]
    RespError(#[from] RespError),

    #[error("Failed to open RDB file")]
    IOError(#[from] tokio::io::Error),
}

#[derive(Debug, Clone)]
pub struct RdbString(pub String);

impl RdbString {
    pub fn parse(input: &[u8]) -> Result<(Self, &[u8]), RdbError> {
        let u8_case = input[1].to_string();
        let u16_case = u16::from_le_bytes([input[1], input[2]]).to_string();
        let u32_case = u32::from_le_bytes([input[1], input[2], input[3], input[4]]).to_string();
        let (value, rest) = match input[0] >> 6 {
            0 => (
                str::from_utf8(&input[1..(input[0] & 0b00111111) as usize + 1]),
                &input[(input[0] & 0b00111111) as usize + 1..],
            ),
            1 => (
                str::from_utf8(
                    (&input[1..u16::from_be_bytes([input[0] & 0b00111111, input[1]]) as usize]),
                ),
                &input[u16::from_be_bytes([input[0] & 0b00111111, input[1]]) as usize..],
            ),
            2 => (
                str::from_utf8(
                    &input
                        [5..u32::from_be_bytes([input[1], input[2], input[3], input[4]]) as usize],
                ),
                &input[u32::from_be_bytes([input[1], input[2], input[3], input[4]]) as usize..],
            ),
            3 => match (input[0] & 0b00111111) {
                0xC0 => (Ok(u8_case.as_str()), &input[1..]),
                0xC1 => (Ok(u16_case.as_str()), &input[2..]),
                0xC2 => (Ok(u32_case.as_str()), &input[4..]),
                0xC3 => todo!(),
                n => {
                    return Err(RdbError::RdbMetadataParserError);
                }
            },
            _ => unreachable!(),
        };

        Ok((Self(value?.to_string()), rest))
    }

    pub fn len(&self) -> usize {
        if self.0.len() <= (u8::MAX << 6) as usize {
            self.0.len() + 1
        } else if self.0.len() <= (u16::MAX << 14) as usize {
            self.0.len() + 2
        } else if self.0.len() <= u32::MAX as usize {
            self.0.len() + 5
        } else {
            0
        }
    }
}

#[derive(Debug)]
pub struct Rdb {
    header: RdbHeader,
    metadata: RdbMetadata,
    pub database: Db,
    pub expiries: Expiries,
}

#[derive(Debug)]
pub struct RdbHeader {
    magic: String,
    version: u32,
}

impl Default for RdbHeader {
    fn default() -> Self {
        Self {
            magic: "REDIS".to_string(),
            version: 11,
        }
    }
}

impl Rdb {
    pub async fn new(config: &Config) -> Result<Self, RdbError> {
        if let Some(dir) = &config.dir {
            if let Some(dbfilename) = &config.dbfilename {
                let mut path = PathBuf::from_str(dir).unwrap();
                path.push(dbfilename);
                let mut file = tokio::fs::File::open(path).await?;
                let mut buf = vec![];
                file.read_to_end(&mut buf).await.unwrap();
                return Self::decode(&buf);
            }
        }
        Err(RdbError::RdbConfigError)
    }
    pub async fn encode_db(&self) -> Vec<u8> {
        let mut buf = vec![START_DB_SECTION, 0, DB_SIZE_FLAG];
        let kv_size = self.database.read().await.len();
        let exp_size = self.expiries.read().await.len();
        write!(buf, "{kv_size}{exp_size}");
        for (key, value) in self.database.read().await.iter() {
            buf.push(0); // flag: string, TODO: handle all types
            buf.extend(key.clone().encode());
            let resp: Resp<'_> = value.clone().try_into().unwrap();
            buf.extend(resp.encode());
            if let Some(expiry) = self.expiries.read().await.get(key) {
                buf.push(HAS_EXPIRY_FLAG);
                // TODO handle actual timestamps
                buf.extend(1713824559637u64.to_le_bytes());
            }
        }
        buf
    }

    pub async fn encode(self) -> Vec<u8> {
        let mut buf = vec![];
        let mut db = self.encode_db().await;
        buf.extend(Into::<Vec<u8>>::into(self.header));
        buf.extend(Into::<Vec<u8>>::into(self.metadata));
        buf.extend(db);
        buf
    }

    pub fn decode(input: &[u8]) -> Result<Self, RdbError> {
        let header = RdbHeader::try_from(input)?;
        let metadata = RdbMetadata::try_from(&input[METADATA_OFFSET..])?;
        let db_start = input
            .iter()
            .position(|b| *b == START_DB_SECTION)
            .unwrap_or(input.len());
        let (database, expiries) = Self::decode_db(&input[db_start..])?;
        Ok(Self {
            header,
            metadata,
            database,
            expiries,
        })
    }

    pub fn decode_db(input: &[u8]) -> Result<(Db, Expiries), RdbError> {
        let mut db = HashMap::new();
        let mut expiries = HashMap::new();

        let (byte, mut rst) = input
            .split_first()
            .ok_or(RdbError::RdbDatabaseParserError)?;
        assert_eq!(*byte, START_DB_SECTION);
        let (byte, rst) = rst.split_first().ok_or(RdbError::RdbDatabaseParserError)?;
        assert_eq!(*byte, 0);
        let (byte, rst) = rst.split_first().ok_or(RdbError::RdbDatabaseParserError)?;
        assert_eq!(*byte, DB_SIZE_FLAG);
        let (db_size, rst) = rst.split_first().ok_or(RdbError::RdbDatabaseParserError)?;
        let (expiry_size, mut rst) = rst.split_first().ok_or(RdbError::RdbDatabaseParserError)?;

        fn decode_inner<'input>(
            input: &'input [u8],
            db: &mut InnerDb,
            expiries: &mut InnerExpiries,
        ) -> Option<&'input [u8]> {
            let (type_value, mut rest) = input.split_first()?;
            let mut expiry = None;
            let mut pair_type = 0;
            match type_value {
                0xFC => {
                    expiry = Some(u64::from_le_bytes(rest[..8].try_into().unwrap()) as i64);
                    pair_type = rest[8];
                    rest = &rest[9..];
                }
                0xFD => {
                    expiry = Some(u32::from_le_bytes(rest[..4].try_into().unwrap()) as i64);
                    pair_type = rest[4];
                    rest = &rest[5..];
                }
                n => {
                    // Otherwise this should be a type
                    pair_type = *n;
                }
            }
            let (key, rest) = RdbString::parse(rest).ok()?;
            let (value, rest) = RdbString::parse(rest).ok()?; // TODO: parse value based on type
            db.insert(key.clone().into(), value.into());
            if let Some(expiry) = expiry {
                expiries.insert(key.into(), expiry);
            }
            Some(rest)
        }

        for i in 0..*db_size {
            rst = decode_inner(rst, &mut db, &mut expiries)
                .ok_or(RdbError::RdbDatabaseParserError)?;
        }

        Ok((Arc::new(RwLock::new(db)), Arc::new(RwLock::new(expiries))))
    }
}

impl From<RdbHeader> for Vec<u8> {
    fn from(value: RdbHeader) -> Self {
        let mut buf = vec![];
        buf.extend(value.magic.as_bytes());
        write!(buf, "{:04}", value.version).unwrap();
        buf
    }
}

impl TryFrom<Vec<u8>> for RdbHeader {
    type Error = RdbError;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let magic =
            std::str::from_utf8(value.get(..5).ok_or(RdbError::RdbHeaderParserError)?)?.to_string();
        let version = std::str::from_utf8(value.get(5..8).ok_or(RdbError::RdbHeaderParserError)?)?;
        let version: u32 = version.parse()?;
        Ok(Self { magic, version })
    }
}

impl TryFrom<&[u8]> for RdbHeader {
    type Error = RdbError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let magic =
            std::str::from_utf8(value.get(..5).ok_or(RdbError::RdbHeaderParserError)?)?.to_string();
        let version = std::str::from_utf8(value.get(5..9).ok_or(RdbError::RdbHeaderParserError)?)?;
        let version: u32 = version.parse()?;
        Ok(Self { magic, version })
    }
}

#[derive(Debug, Default)]
pub struct RdbMetadata {
    attributes: Vec<RdbString>,
}

impl RdbMetadata {
    pub fn len(&self) -> usize {
        self.attributes.iter().fold(0, |acc, next| acc + next.len())
    }
}

impl From<RdbMetadata> for Vec<u8> {
    fn from(value: RdbMetadata) -> Self {
        let mut buf = vec![];
        buf.push(METADATA_START);
        for attr in value.attributes {
            let attr: Vec<u8> = attr.0.as_bytes().to_vec();
            buf.extend(attr);
        }
        buf
    }
}
impl TryFrom<&[u8]> for RdbMetadata {
    type Error = RdbError;
    fn try_from(mut input: &[u8]) -> Result<Self, Self::Error> {
        assert_eq!(input[0], METADATA_START);
        input = &input[1..];
        let mut attributes = vec![];
        while input[0] != SELECTDB {
            if let Ok((value, rest)) = RdbString::parse(&input) {
                attributes.push(value);
                input = rest;
            } else {
                input = &input[input.iter().position(|b| *b == SELECTDB).unwrap_or(0)..];
                break;
            }
        }
        // let attr: MetadataAttribute = TryFrom::<&[u8]>::try_from(&value[1..])?; // TODO parse more attributes
        Ok(Self { attributes })
    }
}

#[derive(Debug)]
pub struct MetadataAttribute {
    name: String,
    value: String,
}

impl Default for MetadataAttribute {
    fn default() -> Self {
        Self {
            name: REDIS_VER.to_string(),
            value: REDIS_VER_VALUE.to_string(),
        }
    }
}

impl From<MetadataAttribute> for Vec<u8> {
    fn from(value: MetadataAttribute) -> Self {
        let mut buf = vec![];
        buf.extend(value.name.as_bytes());
        buf.extend(value.value.as_bytes());
        buf
    }
}
impl TryFrom<Vec<u8>> for MetadataAttribute {
    type Error = RdbError;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        TryFrom::<&[u8]>::try_from(&value)
    }
}

impl TryFrom<&[u8]> for MetadataAttribute {
    type Error = RdbError;
    fn try_from(mut value: &[u8]) -> Result<Self, Self::Error> {
        let u8_case = value[1].to_string();
        let u16_case = u16::from_le_bytes([value[1], value[2]]).to_string();
        let u32_case = u32::from_le_bytes([value[1], value[2], value[3], value[4]]).to_string();
        let name = match value[0] >> 6 {
            0 => str::from_utf8(&value[1..(value[0] & 0b00111111) as usize + 1]),
            1 => str::from_utf8(
                &value[1..u16::from_be_bytes([value[0] & 0b00111111, value[1]]) as usize],
            ),
            2 => str::from_utf8(
                &value[5..u32::from_be_bytes([value[1], value[2], value[3], value[4]]) as usize],
            ),
            3 => match (value[0] & 0b00111111) {
                0xC0 => Ok(u8_case.as_str()),
                0xC1 => Ok(u16_case.as_str()),
                0xC2 => Ok(u32_case.as_str()),
                0xC3 => todo!(),
                _ => todo!(),
            },
            _ => unreachable!(),
        };
        todo!()
        // Ok(Self { name, value })
    }
}
