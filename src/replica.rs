use std::{
    borrow::Cow,
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::{
    command::Command, config::Config, connection::ConnectionError, resp::Resp, Db, Expiries,
};

#[derive(Debug)]
pub struct Replica {
    pub addr: SocketAddr,
    db: Db,
    expiries: Expiries,
    config: Arc<Config>,
    bytes_processed: usize,
}

impl Replica {
    pub fn new(
        addr: String,
        port: String,
        db: Db,
        expiries: Expiries,
        config: Arc<Config>,
    ) -> Self {
        let addr: SocketAddr = format!(
            "{}:{}",
            if &addr == "localhost" {
                "127.0.0.1"
            } else {
                "localhost"
            },
            port
        )
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
        Self {
            addr,
            db,
            expiries,
            config,
            bytes_processed: 0,
        }
    }
    pub async fn start(&mut self) {
        let mut client = TcpStream::connect(self.addr).await.unwrap();
        let ping: Resp<'_> = Command::Ping.into();
        let _ = client.write_all(&ping.encode()).await;
        let mut buf = Vec::with_capacity(4096);
        let _ = client.read_buf(&mut buf).await.unwrap();
        let replconf_port: Resp<'_> = Command::ReplConf(
            Resp::bulk_string("listening-port"),
            Resp::BulkString(Cow::Owned(self.config.port.to_string())),
        )
        .into();
        let _ = client.write_all(&replconf_port.encode()).await;
        buf.clear();
        let _ = client.read_buf(&mut buf).await.unwrap();
        let replconf_capa: Resp<'_> =
            Command::ReplConf(Resp::bulk_string("capa"), Resp::bulk_string("psync2")).into();
        let _ = client.write_all(&replconf_capa.encode()).await;
        buf.clear();
        let _ = client.read_buf(&mut buf).await;
        let psync: Resp<'_> =
            Command::Psync(Resp::bulk_string("?"), Resp::bulk_string("-1")).into();
        let _ = client.write_all(&psync.encode()).await;
        buf.clear();
        let _ = client.read_buf(&mut buf).await;

        let _ = self.handle(client).await;
    }

    pub async fn handle(&mut self, mut tcp: TcpStream) -> Result<(), ConnectionError> {
        let mut buf = [0; 512];
        loop {
            let n = tcp.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let mut rest = buf.as_slice();
            while !rest.is_empty() {
                match Command::parse(rest) {
                    Ok((c, new_rest)) => {
                        dbg!(&c);
                        self.handle_command(c, &mut tcp).await?;
                        self.bytes_processed += rest.len() - new_rest.len();
                        rest = new_rest;
                    }
                    Err(err) => {
                        tcp.write_all(
                            &Resp::SimpleError(Cow::Borrowed("unknown command")).encode(),
                        )
                        .await?;
                        eprintln!("{}", err);
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn handle_command<'c>(
        &mut self,
        command: Command<'c>,
        tcp: &mut TcpStream,
    ) -> Result<(), ConnectionError> {
        match &command {
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
            }
            Command::ReplConf(key, _value) => {
                match key {
                    Resp::BulkString(cow) => {
                        dbg!(&cow.to_string());
                    }
                    n => {
                        dbg!(&n);
                    }
                }
                // let key = key.expect_bulk_string().map(|key| key.clone().into_owned());
                // if let Some(key) = key {
                //     if key.as_str() == "GETACK" {
                //         let resp: Resp<'_> = Command::ReplConf(
                //             Resp::bulk_string("ACK"),
                //             Resp::BulkString(Cow::Owned(self.bytes_processed.to_string())),
                //         )
                //         .into();
                //         tcp.write_all(&resp.encode()).await?;
                //     }
                // }
            }
            _ => {
                return Ok(());
                // As a replica we should not ever receive read commands
            }
        };

        Ok(())
    }
}
