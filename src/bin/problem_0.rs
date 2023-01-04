use std::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let tcp_listener = TcpListener::bind("127.0.0.1:8080").await?;

    println!("Running server for Problem 0 on port 8080");

    loop {
        let (tcp_socket_stream, _) = tcp_listener.accept().await?;
        let (mut tcp_socket_reader, mut tcp_socket_writer) = tcp_socket_stream.into_split();
        echo(&mut tcp_socket_reader, &mut tcp_socket_writer).await?;
    }
}

async fn echo(reader: &mut OwnedReadHalf, writer: &mut OwnedWriteHalf) -> Result<()> {
    let mut payload_buffer = Vec::new();
    reader.read_to_end(&mut payload_buffer).await?;

    writer.write_all(&payload_buffer).await?;

    Ok(())
}
