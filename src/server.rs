use clap::Parser;
use std::borrow::Cow;
use std::net::SocketAddrV4;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use tokio::{net::TcpStream, sync::RwLock};

use crate::connection::ConnectionError;
use crate::replica::Replica;
use crate::REPLICATION_ID;
use crate::{command::Command, config::Config, connection::Connection, rdb::Rdb, resp::Resp};

pub type Db = Arc<RwLock<HashMap<Resp<'static>, Resp<'static>>>>;
pub type Expiries = Arc<RwLock<HashMap<Resp<'static>, i64>>>;

#[derive(Debug)]
pub struct Server {
    config: Arc<Config>,
    address: SocketAddrV4,
    db: Db,
    expiries: Expiries,
    master_replication_id: String,
    is_replica: bool,
    propagation_sender: BroadcastSender<Command<'static>>,
    propagation_receiver: BroadcastReceiver<Command<'static>>,
}

impl Server {
    pub fn new() -> Self {
        let config = Arc::new(Config::parse());
        let address = SocketAddrV4::new([127, 0, 0, 1].try_into().unwrap(), config.port);
        let db: Db = Arc::new(RwLock::new(HashMap::new()));
        let expiries: Expiries = Arc::new(RwLock::new(HashMap::new()));

        let master_replication_id = REPLICATION_ID.to_string();
        let is_replica = config.replicaof.is_some();
        let (propagation_sender, propagation_receiver) = broadcast::channel(32);
        Self {
            config,
            address,
            db,
            expiries,
            master_replication_id,
            is_replica,
            propagation_sender,
            propagation_receiver,
        }
    }

    pub async fn initialize(&mut self) {
        self.initialize_rdb().await;
        self.initialize_expiration_handlers().await;
        if self.is_replica {
            self.initialize_replication_slave().await;
        }
    }

    pub async fn initialize_rdb(&mut self) {
        if self.config.dir.is_some() && self.config.dbfilename.is_some() {
            match Rdb::new(&self.config).await {
                Ok(rdb) => {
                    self.db = rdb.database;
                    self.expiries = rdb.expiries;
                }
                Err(err) => {
                    println!("Rdb error: {err}");
                }
            }
        }
    }

    pub async fn initialize_expiration_handlers(&mut self) {
        let expiries_map = self.expiries.read().await;
        let entries = expiries_map.clone().into_iter();

        for (key, expiry) in entries {
            let expiries = self.expiries.clone();
            let db = self.db.clone();
            tokio::spawn(async move {
                let expiring_at = SystemTime::UNIX_EPOCH + Duration::from_millis(expiry as u64);
                let duration = expiring_at.duration_since(SystemTime::now());

                if let Ok(duration) = duration {
                    tokio::time::sleep(duration).await;
                }

                db.write().await.remove(&key);
                expiries.write().await.remove(&key);
            });
        }
    }

    pub async fn initialize_replication_slave(&mut self) {
        if let Some((addr, port)) = self.config.replicaof.clone().and_then(|addr| {
            let (addr, port) = addr.split_once(" ")?;

            Some((addr.to_string(), port.to_string()))
        }) {
            let config = self.config.clone();
            let db = self.db.clone();
            let expiries = self.expiries.clone();
            tokio::spawn(async move {
                let mut replica = Replica::new(addr, port, db, expiries, config);
                replica.start().await;
            });
        }
    }

    pub async fn start(self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect(&format!("Can not listen to port {}", self.config.port));
        println!("Listening on port: {}", self.config.port);
        loop {
            let db = self.db.clone();
            let expiries = self.expiries.clone();
            let propagation_sender = self.propagation_sender.clone();
            let mut connection = Connection::new(
                listener.accept().await.unwrap(),
                db,
                expiries,
                self.config.clone(),
                self.master_replication_id.clone(),
                propagation_sender,
            );
            let mut propagation_receiver = self.propagation_receiver.resubscribe();
            tokio::spawn(async move {
                connection.handle().await?;
                if connection.is_promoted_to_replica {
                    println!("connection is promoted to replica");
                    let getack: Resp<'_> =
                        Command::ReplConf(Resp::bulk_string("GETACK"), Resp::bulk_string("*"))
                            .into();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let n = connection.write_all(&getack.encode()).await;
                    tokio::spawn(async move {
                        while let Ok(command) = propagation_receiver.recv().await {
                            let resp: Resp<'_> = command.into();
                            println!(
                                "Propagating command {:?} to replica {}",
                                &resp,
                                &connection.addr.port()
                            );
                            let _ = connection.write_all(&resp.encode()).await;
                        }
                    });
                }

                Result::<(), ConnectionError>::Ok(())
            });
        }
    }
}
