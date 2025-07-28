use crate::{data::Value, resp::Resp};
use indexmap::IndexMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Invalid format for stream id was provided")]
    MallformedStreamId,
}

impl StreamId {}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct StreamId {
    pub milliseconds: usize,
    pub sequence_number: usize,
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

    pub fn insert(&mut self, id: StreamId, key: String, value: Value) {
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
    }
}
