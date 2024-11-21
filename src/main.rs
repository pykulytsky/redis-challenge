#![allow(unused_imports)]
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::{connection::Connection, resp::Resp};

mod command;
mod connection;
mod resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Can not listen to port 6379");
    loop {
        let connection = Connection::new(listener.accept().await.unwrap());
        tokio::spawn(async move { connection.handle().await });
    }
}
