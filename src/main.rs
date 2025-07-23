#![allow(unused_imports)]
use clap::Parser;
use std::{
    collections::HashMap,
    net::SocketAddrV4,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::{connection::Connection, rdb::Rdb, resp::Resp};

mod command;
mod config;
mod connection;
mod rdb;
mod resp;

pub type Db = Arc<RwLock<HashMap<Resp<'static>, Resp<'static>>>>;
pub type Expiries = Arc<RwLock<HashMap<Resp<'static>, i64>>>;

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

#[tokio::main]
async fn main() {
    let config = config::Config::parse();
    let mut db: Db = Arc::new(RwLock::new(HashMap::new()));
    let mut expiries: Expiries = Arc::new(RwLock::new(HashMap::new()));

    match Rdb::new(&config).await {
        Ok(rdb) => {
            db = rdb.database;
            expiries = rdb.expiries;
        }
        Err(err) => {
            println!("Rdb error: {err}");
        }
    }

    {
        let expiries_map = expiries.read().await;
        let entries = expiries_map.clone().into_iter();

        for (key, expiry) in entries {
            let expiries = expiries.clone();
            let db = db.clone();
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

    let address = SocketAddrV4::new([127, 0, 0, 1].try_into().unwrap(), config.port);
    let listener = TcpListener::bind(address)
        .await
        .expect(&format!("Can not listen to port {}", config.port));
    loop {
        let db = db.clone();
        let expiries = expiries.clone();
        let connection = Connection::new(
            listener.accept().await.unwrap(),
            db,
            expiries,
            config.clone(),
            REPLICATION_ID,
        );
        tokio::spawn(async move { connection.handle().await });
    }
}
