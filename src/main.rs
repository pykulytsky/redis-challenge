#![allow(unused_imports)]
use clap::Parser;
use std::{collections::HashMap, sync::Arc};
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

#[tokio::main]
async fn main() {
    let config = config::Config::parse();
    let mut db: Db = Arc::new(RwLock::new(HashMap::new()));
    let mut expiries: Expiries = Arc::new(RwLock::new(HashMap::new()));

    if let Ok(rdb) = Rdb::new(&config).await {
        db = rdb.database;
        expiries = rdb.expiries;
    }
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Can not listen to port 6379");
    loop {
        let db = db.clone();
        let expiries = expiries.clone();
        let connection = Connection::new(
            listener.accept().await.unwrap(),
            db,
            expiries,
            config.clone(),
        );
        tokio::spawn(async move { connection.handle().await });
    }
}
