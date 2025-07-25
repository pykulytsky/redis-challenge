use core::str;
use std::{
    borrow::Cow,
    collections::HashMap,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncRead};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::{
    command::{
        Command, CommandError,
        ConfigItem::{DbFileName, Dir},
    },
    config::Config,
    resp::{Resp, RespError},
    Db, Expiries,
};

#[derive(Debug)]
pub struct Connection<'s> {
    pub tcp: TcpStream,
    pub addr: SocketAddr,
    db: Db,
    expiries: Expiries,
    config: Config,
    server_replication_id: &'s str,
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

impl<'s> Connection<'s> {
    pub fn new(
        (tcp, addr): (TcpStream, SocketAddr),
        db: Db,
        expiries: Expiries,
        config: Config,
        server_replication_id: &'s str,
    ) -> Self {
        Self {
            tcp,
            addr,
            db,
            expiries,
            config,
            server_replication_id,
        }
    }

    pub async fn handle(mut self) -> Result<(), ConnectionError> {
        println!("accepted new connection: {}", self.addr);
        let mut buf = [0; 512];
        loop {
            let n = self.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let mut rest = buf.as_slice();
            while !rest.is_empty() {
                match Command::parse(rest) {
                    Ok((c, new_rest)) => {
                        self.handle_command(c).await?;
                        rest = new_rest;
                    }
                    Err(err) => {
                        self.write_all(
                            &Resp::SimpleError(Cow::Borrowed("unknown command")).encode(),
                        )
                        .await?;
                        eprintln!("{}", err);
                        break;
                    }
                }
            }
        }
        self.tcp.shutdown().await.unwrap();
        Ok(())
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
            Command::Set(key, value, expiry) => {
                self.db
                    .write()
                    .await
                    .insert(key.clone().into_owned(), value.clone().into_owned());
                if let Some(expiry) = expiry {
                    let expiry = *expiry;
                    let db = self.db.clone();
                    self.expiries
                        .write()
                        .await
                        .insert(key.clone().into_owned(), expiry);
                    let key = key.clone().into_owned();
                    let expiries = self.expiries.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(expiry as u64)).await;
                        db.write().await.remove(&key);
                        expiries.write().await.remove(&key);
                    });
                }
                Resp::bulk_string("OK")
            }
            Command::ConfigGet(item) => match item {
                Dir if self.config.dir.is_some() => Resp::array(vec![
                    Resp::bulk_string("dir"),
                    Resp::BulkString(Cow::Owned(self.config.clone().dir.unwrap())),
                ]),
                DbFileName if self.config.dbfilename.is_some() => Resp::array(vec![
                    Resp::bulk_string("dbfilename"),
                    Resp::BulkString(Cow::Owned(self.config.clone().dbfilename.unwrap())),
                ]),
                _ => todo!(),
            },
            Command::Keys(key) => {
                let keys: Vec<Resp<'_>> = self
                    .db
                    .read()
                    .await
                    .keys()
                    .filter(|_k| {
                        let Some(key) = key.expect_bulk_string() else {
                            return false;
                        };

                        if key == "*" {
                            return true;
                        } else {
                            return false; // TODO filter by key
                        }
                    })
                    .cloned()
                    .collect();
                Resp::Array(keys)
            }
            Command::Save => {
                todo!()
            }
            Command::Info(_parameter) => {
                let is_replica = self.config.replicaof.is_some();
                let role = if is_replica {
                    "role:slave\r\n"
                } else {
                    "role:master\r\n"
                };
                let master_replid = format!("master_replid:{}\r\n", self.server_replication_id);
                let master_repl_offset = "master_repl_offset:0\r\n";
                Resp::BulkString(Cow::Owned(format!(
                    "{}{}{}",
                    role, master_replid, master_repl_offset
                )))
            }
            Command::ReplConf(_, _) => Resp::bulk_string("OK"),
            Command::Psync(_master_replication_id, _master_offset) => {
                let fullresync = Resp::SimpleString(Cow::Owned(format!(
                    "FULLRESYNC {} 0",
                    self.server_replication_id
                )));
                self.write_all(&fullresync.encode()).await?;
                let empty_rdb: &[u8] = &[
                    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65,
                    0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
                    0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0,
                    0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65,
                    0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4,
                    0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0,
                    0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
                ];
                let mut rdb = vec![];
                rdb.extend_from_slice(format!("${}\r\n", empty_rdb.len()).as_bytes());
                rdb.extend_from_slice(empty_rdb);
                self.write_all(&rdb).await?;
                return Ok(());
            }
        };
        self.write_all(&resp.encode()).await?;
        Ok(())
    }
}

impl<'s> AsyncWrite for Connection<'s> {
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

impl<'s> AsyncRead for Connection<'s> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let tcp = Pin::new(&mut self.tcp);
        TcpStream::poll_read(tcp, cx, buf)
    }
}
