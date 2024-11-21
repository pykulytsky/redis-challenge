#![allow(unused_imports)]
use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::{connection::Connection, resp::Resp};

mod command;
mod connection;
mod resp;

pub type Db = Arc<RwLock<HashMap<Resp<'static>, Resp<'static>>>>;

#[tokio::main]
async fn main() {
    let db: Db = Arc::new(RwLock::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Can not listen to port 6379");
    loop {
        let db = db.clone();
        let connection = Connection::new(listener.accept().await.unwrap(), db);
        tokio::spawn(async move { connection.handle().await });
    }
}
