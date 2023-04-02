use std::borrow::Cow;
use std::io::{Error as IO_Error, Result as IO_Result};

use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> IO_Result<()> {
    let tcp_listener = TcpListener::bind("0.0.0.0:8080").await?;
    let mut conn_counter = 0;

    println!("Running server for Problem 3 on port 8080");

    loop {
        let (tcp_socket_stream, client_address) = tcp_listener.accept().await?;
        let (tcp_socket_reader, tcp_socket_writer) = tcp_socket_stream.into_split();
        conn_counter += 1;
        let current_connection = conn_counter;
        println!("Established connection {current_connection} from {client_address}");

        tokio::spawn(async move {
            let mut user_message_lines = BufReader::new(tcp_socket_reader).lines();
            let mut user_message_writer = BufWriter::new(tcp_socket_writer);

            // Connect to remote chat tcp socket for this user
            let chat_socket_stream = TcpStream::connect("chat.protohackers.com:16963").await?;
            let (chat_socket_reader, chat_socket_writer) = chat_socket_stream.into_split();
            let mut chat_message_lines = BufReader::new(chat_socket_reader).lines();
            let mut chat_message_writer = BufWriter::new(chat_socket_writer);

            loop {
                tokio::select! {
                        user_message_line = user_message_lines.next_line() => match user_message_line {
                            Ok(Some(user_message)) => {
                                // New user message -> Forward to chat
                                println!("[{current_connection}] User wrote message: {user_message}");
                                forward_message(user_message, &mut chat_message_writer).await?;
                            },
                            _ => break,
                        },
                        chat_message_line = chat_message_lines.next_line() => match chat_message_line {
                            Ok(Some(chat_message)) => {
                                // New chat message -> Forward to user
                                println!("[{current_connection}] Chat wrote message: {chat_message}");
                                forward_message(chat_message, &mut user_message_writer).await?;
                            },
                            _ => break,
                }
                    }
            }

            println!("[{current_connection}] User disconnected");

            Ok::<(), IO_Error>(())
        });
    }
}

async fn forward_message(
    message: String,
    socket_writer: &mut BufWriter<OwnedWriteHalf>,
) -> IO_Result<()> {
    let message = replace_boguscoin_address(&message);

    socket_writer
        .write_all(format!("{message}\n").as_bytes())
        .await?;
    socket_writer.flush().await?;
    Ok(())
}

fn replace_boguscoin_address(message: &str) -> Cow<str> {
    let address_regex = Regex::new(r"\b7[a-zA-Z0-9]{25,34}\b").unwrap();
    address_regex.replace_all(message, "${1}7YWHMfk9JZe0LM0g1ZauHuiSxhI${3}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_boguscoin_address() {
        assert_eq!(
            replace_boguscoin_address("7F1u3wSD5RbOHQmupo9nx4TnhQ"),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
        assert_eq!(
            replace_boguscoin_address("Address: 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX"),
            "Address: 7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
        assert_eq!(
            replace_boguscoin_address(
                "My wallet address is 7LOrwbDlS8NujgjddyogWgIM93MV5N2VR and I accept Boguscoin"
            ),
            "My wallet address is 7YWHMfk9JZe0LM0g1ZauHuiSxhI and I accept Boguscoin"
        );
        assert_eq!(
            replace_boguscoin_address(
                "7adNeSwJkMakpEcln9HEtthSRtxdmEHOT8T is the address for the payment"
            ),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI is the address for the payment"
        );
        assert_eq!(
            replace_boguscoin_address(
                "You can use 7F1u3wSD5RbOHQmupo9nx4TnhQ or 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX for your payment"
            ),
            "You can use 7YWHMfk9JZe0LM0g1ZauHuiSxhI or 7YWHMfk9JZe0LM0g1ZauHuiSxhI for your payment"
        );
        assert_eq!(
            replace_boguscoin_address("abcdefghijklmnopqrstuvwxyz"),
            "abcdefghijklmnopqrstuvwxyz"
        );
        assert_eq!(
            replace_boguscoin_address("7abcdefghijklmnopqrstuvwx"),
            "7abcdefghijklmnopqrstuvwx"
        );
        assert_eq!(
            replace_boguscoin_address("7abcdefghijklmnopqrstuvwxy"),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
        assert_eq!(
            replace_boguscoin_address("7abcdefghijklmnopqrstuvwxyz12345678"),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
        assert_eq!(
            replace_boguscoin_address("7abcdefghijklmnopqrstuvwxyz123456789"),
            "7abcdefghijklmnopqrstuvwxyz123456789"
        );
    }
}
