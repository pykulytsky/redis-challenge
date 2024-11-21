use crate::resp::{Resp, RespError};
use thiserror::Error;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
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

impl Command {
    pub fn parse(input: &[u8]) -> Result<Self, CommandError> {
        use Command::*;
        use CommandError::*;
        let packet = Resp::parse(input)?;
        match packet {
            Resp::Array(array) => match array.first().ok_or(IncorrectFormat)? {
                Resp::BulkString(c) => match c {
                    &"PING" => Ok(Ping),
                    &"ECHO" => {
                        let arg = array.get(1).ok_or(IncorrectFormat)?;
                        match arg {
                            Resp::BulkString(s) => Ok(Echo(s.to_string())),
                            _ => Err(IncorrectFormat),
                        }
                    }
                    c => Err(UnsupportedCommand(c.to_string())),
                },
                _ => Err(IncorrectFormat),
            },
            _ => Err(IncorrectFormat),
        }
    }
}
