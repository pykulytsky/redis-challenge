#![allow(unused_imports)]
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::resp::Resp;

mod command;
mod resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Can not listen to port 6379");
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection: {}", stream.1);
                stream
                    .0
                    .write_all(&Resp::SimpleString("PONG").encode())
                    .await
                    .unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
