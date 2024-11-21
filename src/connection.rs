use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncRead};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::TcpStream;

use crate::resp::{Resp, RespError};

#[derive(Debug)]
pub struct Connection {
    pub tcp: TcpStream,
    pub addr: SocketAddr,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("IO error")]
    Io(#[from] tokio::io::Error),

    #[error("Protocol error")]
    Protocol(#[from] RespError),
}

impl Connection {
    pub fn new((tcp, addr): (TcpStream, SocketAddr)) -> Self {
        Self { tcp, addr }
    }

    pub async fn handle(mut self) -> Result<(), ConnectionError> {
        println!("accepted new connection: {}", self.addr);
        let mut buf = Vec::with_capacity(512);
        loop {
            let n = self.read_buf(&mut buf).await?;
            if n == 0 {
                continue;
            }
            // let data = Resp::parse(&buf[..n])?;
            self.write_all(&Resp::SimpleString("PONG").encode()).await?;
            buf.clear();
        }
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
