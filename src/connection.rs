use std::{
    borrow::Cow,
    collections::HashMap,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncRead};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::{
    command::{Command, CommandError},
    resp::{Resp, RespError},
    Db,
};

#[derive(Debug)]
pub struct Connection {
    pub tcp: TcpStream,
    pub addr: SocketAddr,
    db: Db,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("IO error")]
    Io(#[from] tokio::io::Error),

    #[error("Protocol error")]
    Protocol(#[from] RespError),

    #[error("Command error")]
    Command(#[from] CommandError),
}

impl Connection {
    pub fn new((tcp, addr): (TcpStream, SocketAddr), db: Db) -> Self {
        Self { tcp, addr, db }
    }

    pub async fn handle(mut self) -> Result<(), ConnectionError> {
        println!("accepted new connection: {}", self.addr);
        let mut buf = Vec::with_capacity(512);
        loop {
            let n = self.read_buf(&mut buf).await?;
            if n == 0 {
                continue;
            }
            match Command::parse(&buf[..n]) {
                Ok(c) => {
                    self.handle_command(c).await?;
                }
                Err(err) => {
                    self.write_all(&Resp::SimpleError(Cow::Borrowed("Invalid command")).encode())
                        .await?;
                    eprintln!("{}", err);
                }
            }
            buf.clear();
        }
    }

    pub async fn handle_command<'c>(
        &mut self,
        command: Command<'c>,
    ) -> Result<(), ConnectionError> {
        let resp = match &command {
            Command::Ping => Resp::simple_string("PONG"),
            Command::Echo(msg) => Resp::bulk_string(msg),
            Command::Get(key) => self
                .db
                .read()
                .await
                .get(key)
                .cloned()
                .unwrap_or(Resp::bulk_string("")),
            Command::Set(key, value) => {
                self.db
                    .write()
                    .await
                    .insert(key.clone().to_owned(), value.clone().to_owned());
                Resp::bulk_string("OK")
            }
        };
        self.write_all(&resp.encode()).await?;
        Ok(())
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let tcp = Pin::new(&mut self.tcp);
        TcpStream::poll_write(tcp, cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let tcp = Pin::new(&mut self.tcp);
        TcpStream::poll_flush(tcp, cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let tcp = Pin::new(&mut self.tcp);
        TcpStream::poll_shutdown(tcp, cx)
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let tcp = Pin::new(&mut self.tcp);
        TcpStream::poll_read(tcp, cx, buf)
    }
}
