use std::{borrow::Cow, usize};

use crate::{data::Value, resp::Resp, utils::get_epoch_ms};
use indexmap::IndexMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Invalid format for stream id was provided")]
    MallformedStreamId,

    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    InvalidStreamId,

    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    ZeroStreamId,

    #[error("Missing sequence number")]
    ShouldGenerateSequenceNumber(usize),

    #[error("Missing milliseconds and sequence number")]
    ShouldGenerateFullId,
}

#[derive(Debug, Clone, Copy, Hash)]
pub struct StreamId {
    pub milliseconds: usize, // Should store it either as u128 or u64
    pub sequence_number: usize,
}

impl StreamId {
    pub const MIN: Self = Self {
        milliseconds: usize::MIN,
        sequence_number: usize::MIN,
    };

    pub const MAX: Self = Self {
        milliseconds: usize::MAX,
        sequence_number: usize::MAX,
    };

    pub fn is_zero(&self) -> bool {
        self.milliseconds == 0 && self.sequence_number == 0
    }

    pub fn cmp_millis(&self, other: &Self) -> bool {
        self.milliseconds == other.milliseconds
    }
}

impl PartialEq for StreamId {
    fn eq(&self, other: &Self) -> bool {
        self.milliseconds == other.milliseconds && self.sequence_number == other.sequence_number
    }
}

impl Eq for StreamId {}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let milliseconds_cmp = self.milliseconds.cmp(&other.milliseconds);
        let cmp = match milliseconds_cmp {
            std::cmp::Ordering::Less | std::cmp::Ordering::Greater => milliseconds_cmp,
            std::cmp::Ordering::Equal => self.sequence_number.cmp(&other.sequence_number),
        };

        Some(cmp)
    }
}

impl Ord for StreamId {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        todo!()
    }
}

impl TryFrom<&Resp<'_>> for StreamId {
    type Error = StreamError;

    fn try_from(resp: &Resp<'_>) -> Result<Self, Self::Error> {
        let Some(resp) = resp.expect_bulk_string() else {
            return Err(StreamError::MallformedStreamId);
        };

        let pair = resp.split_once('-');
        match pair {
            Some(pair) => match pair {
                (milliseconds, "*") => {
                    return Err(StreamError::ShouldGenerateSequenceNumber(
                        milliseconds
                            .parse()
                            .map_err(|_| StreamError::MallformedStreamId)?,
                    ))
                }
                (milliseconds, sequence_number) => {
                    let milliseconds: usize = milliseconds
                        .parse()
                        .map_err(|_| StreamError::MallformedStreamId)?;
                    let sequence_number: usize = sequence_number
                        .parse()
                        .map_err(|_| StreamError::MallformedStreamId)?;
                    Ok(Self {
                        milliseconds,
                        sequence_number,
                    })
                }
            },
            None => {
                if resp == "*" {
                    return Err(StreamError::ShouldGenerateFullId);
                } else {
                    return Err(StreamError::MallformedStreamId);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    inner: IndexMap<StreamId, IndexMap<String, Value>>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            inner: IndexMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        id: &Resp<'_>,
        key: String,
        value: Value,
    ) -> Result<StreamId, StreamError> {
        let id = match StreamId::try_from(id) {
            Ok(id) => id,
            Err(err) => match err {
                StreamError::ShouldGenerateSequenceNumber(milliseconds) => {
                    let sequence_number = match self
                        .inner
                        .keys()
                        .find(|key| key.milliseconds == milliseconds)
                    {
                        Some(id) => id.sequence_number + 1,
                        None => {
                            if milliseconds == 0 {
                                1
                            } else {
                                0
                            }
                        }
                    };

                    StreamId {
                        milliseconds,
                        sequence_number,
                    }
                }
                StreamError::ShouldGenerateFullId => {
                    let milliseconds = get_epoch_ms();
                    let sequence_number = match self.inner.keys().last() {
                        Some(key) => {
                            if key.milliseconds == milliseconds {
                                key.sequence_number + 1
                            } else {
                                0
                            }
                        }
                        None => 0, // We can safely return zero here as milliseconds should never be equal to 0
                    };

                    StreamId {
                        milliseconds,
                        sequence_number,
                    }
                }
                _ => return Err(err),
            },
        };

        if id.is_zero() {
            return Err(StreamError::ZeroStreamId);
        }

        if let Some(last_id) = self.inner.keys().last() {
            if id <= *last_id {
                return Err(StreamError::InvalidStreamId);
            }
        }

        let stream_entry = self.inner.entry(id);
        match stream_entry {
            indexmap::map::Entry::Occupied(mut occupied_entry) => {
                let index_map = occupied_entry.get_mut();
                index_map.insert(key, value);
            }
            indexmap::map::Entry::Vacant(vacant_entry) => {
                let mut index_map = IndexMap::new();
                index_map.insert(key, value);
                vacant_entry.insert(index_map);
            }
        }

        Ok(id)
    }

    pub fn range(&self, from: &Resp<'_>, to: &Resp<'_>) -> Result<Resp<'static>, StreamError> {
        let from_id = from.try_into().or_else(|e| {
            let key = from
                .expect_bulk_string()
                .ok_or(StreamError::MallformedStreamId)?;
            if key.as_bytes() == b"-" {
                Ok(StreamId::MIN)
            } else {
                Err(e)
            }
        })?;
        let to_id = to.try_into().or_else(|e| {
            let key = from
                .expect_bulk_string()
                .ok_or(StreamError::MallformedStreamId)?;
            if key.as_bytes() == b"+" {
                Ok(StreamId::MAX)
            } else {
                Err(e)
            }
        })?;
        let vec = self
            .inner
            .clone()
            .into_iter()
            .filter(|(id, _)| *id >= from_id && *id <= to_id)
            .map(|(id, value)| (id, value.into_iter()))
            .map(|(id, items)| {
                let mut inner_array = vec![];
                for (key, value) in items {
                    inner_array.push(Resp::BulkString(Cow::Owned(key)));
                    inner_array.push(value.try_into().unwrap());
                }

                Resp::Array(vec![id.into(), Resp::Array(inner_array)])
            })
            .collect();

        Ok(Resp::Array(vec))
    }
}
