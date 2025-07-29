#![allow(unused_imports)]
use clap::Parser;
use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    net::SocketAddrV4,
    process::Output,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    command::Command,
    connection::{Connection, ConnectionError},
    data::Value,
    rdb::Rdb,
    resp::Resp,
    server::Server,
};

mod command;
mod config;
mod connection;
mod data;
mod rdb;
mod replica;
mod resp;
mod server;
mod utils;

pub type InnerDb = HashMap<Resp<'static>, Value>;
pub type InnerExpiries = HashMap<Resp<'static>, i64>;

pub type Db = Arc<RwLock<InnerDb>>;
pub type Expiries = Arc<RwLock<InnerExpiries>>;

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

#[tokio::main]
async fn main() {
    let mut server = Server::new();
    server.initialize().await;
    server.start().await;
}
