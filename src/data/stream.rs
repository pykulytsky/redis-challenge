use crate::{data::Value, resp::Resp};
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
}

#[derive(Debug, Clone, Copy, Hash)]
pub struct StreamId {
    pub milliseconds: usize,
    pub sequence_number: usize,
}

impl StreamId {
    pub fn is_zero(&self) -> bool {
        self.milliseconds == 0 && self.sequence_number == 0
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

        let (milliseconds, sequence_number) = resp
            .split_once("-")
            .and_then(|(left, right)| {
                let left: usize = left.parse().ok()?;
                let right: usize = right.parse().ok()?;
                Some((left, right))
            })
            .ok_or(StreamError::MallformedStreamId)?;

        Ok(Self {
            milliseconds,
            sequence_number,
        })
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

    pub fn insert(&mut self, id: &Resp<'_>, key: String, value: Value) -> Result<(), StreamError> {
        let id = StreamId::try_from(id)?;
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

        Ok(())
    }
}
