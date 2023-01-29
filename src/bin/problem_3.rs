use std::collections::HashSet;
use std::io::{Error as IO_Error, Result as IO_Result};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};

enum UserPreambleError<D> {
    Protocol(D),
    IO(IO_Error),
}

struct ChatRoomClient {
    connection_id: i32,
    user_name: Option<String>,
    socket_reader: Lines<BufReader<OwnedReadHalf>>,
    socket_writer: BufWriter<OwnedWriteHalf>,
    broadcast_receiver: Receiver<(i32, String)>,
    broadcast_sender: Sender<(i32, String)>,
}

impl ChatRoomClient {
    async fn send_message_to_user(&mut self, message: String) -> IO_Result<()> {
        self.socket_writer
            .write_all(format!("{message}\n").as_bytes())
            .await?;
        self.socket_writer.flush().await?;
        Ok(())
    }

    fn broadcast_message(&self, message: String) {
        self.broadcast_sender
            .send((self.connection_id, message))
            .unwrap();
    }

    fn broadcast_join_message(&self) {
        let user_name = self.user_name.as_ref().unwrap();
        self.broadcast_message(format!("* {user_name} has entered the room"))
    }

    fn broadcast_user_message(&self, user_message: String) {
        let user_name = self.user_name.as_ref().unwrap();
        self.broadcast_message(format!("[{user_name}] {user_message}"));
    }

    fn broadcast_disconnect_message(&self) {
        let user_name = self.user_name.as_ref().unwrap();
        self.broadcast_message(format!("* {user_name} has left the room"))
    }

    async fn client_join_preamble(&mut self) -> Result<String, UserPreambleError<String>> {
        // Send user name input prompt
        self.send_message_to_user(String::from(
            "Welcome to budgetchat! What shall I call you?",
        ))
        .await
        .map_err(UserPreambleError::IO)?;

        // Await user name input
        let name_input_line = self
            .socket_reader
            .next_line()
            .await
            .map_err(UserPreambleError::IO)?;
        if let Some(name_input) = name_input_line {
            let user_name = String::from(name_input.trim());
            // Check if input is a valid user name
            if is_valid_name(&user_name) {
                self.user_name = Some(user_name.clone());
                Ok(user_name)
            } else {
                Err(UserPreambleError::Protocol(String::from(
                    "INVALID_USER_NAME",
                )))
            }
        } else {
            Err(UserPreambleError::Protocol(String::from(
                "INVALID_USER_NAME",
            )))
        }
    }

    async fn process_message_interchange(&mut self) -> IO_Result<()> {
        loop {
            tokio::select! {
                user_message_line = self.socket_reader.next_line() => match user_message_line {
                    Ok(Some(user_message)) => {
                        println!("[{}] Wrote message: {user_message}", self.connection_id);
                        self.broadcast_user_message(user_message);
                    },
                    _ => break,
                },
                participant_message = self.broadcast_receiver.recv() => match participant_message {
                    Ok((participant_conn_id, participant_message_text)) => {
                        if self.connection_id != participant_conn_id {
                            self.send_message_to_user(participant_message_text).await?;
                        }
                    }
                    _ => break
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> IO_Result<()> {
    let tcp_listener = TcpListener::bind("0.0.0.0:8080").await?;

    println!("Running server for Problem 3 on port 8080");

    let mut conn_counter = 0;
    let participant_user_names = Arc::new(Mutex::new(HashSet::new()));

    let (broadcast_sender, _) = broadcast::channel(32);

    loop {
        let (tcp_socket_stream, client_address) = tcp_listener.accept().await?;
        conn_counter += 1;
        let current_connection = conn_counter;
        println!("Established connection {current_connection} from {client_address}");

        // Create chat room client for user socket
        let (tcp_socket_reader, tcp_socket_writer) = tcp_socket_stream.into_split();
        let broadcast_sender = broadcast_sender.clone();
        let mut chat_room_client = ChatRoomClient {
            connection_id: current_connection,
            user_name: None,
            socket_reader: BufReader::new(tcp_socket_reader).lines(),
            socket_writer: BufWriter::new(tcp_socket_writer),
            broadcast_receiver: broadcast_sender.subscribe(),
            broadcast_sender,
        };

        let participant_user_names = Arc::clone(&participant_user_names);

        tokio::spawn(async move {
            // Handle new user join protocol
            match chat_room_client.client_join_preamble().await {
                Ok(user_name) => {
                    println!("[{current_connection}] New user joined: {user_name}");
                    // Check if name is already used
                    if let Ok(participant_names_list) =
                        try_join_with_user_name(&user_name, participant_user_names)
                    {
                        // Valid name -> Send list of current participant names to new user
                        chat_room_client
                            .send_message_to_user(format!(
                                "* The room contains: {participant_names_list}"
                            ))
                            .await?;
                    } else {
                        // Duplicate user name -> Close connection
                        println!(
                            "[{}] Name {user_name} is already used. Connection will be closed.",
                            chat_room_client.connection_id
                        );
                        return Ok(());
                    }

                    // Broadcast join message to all participants
                    chat_room_client.broadcast_join_message();
                }
                Err(UserPreambleError::Protocol(error_type)) => {
                    println!("[{current_connection}] Error {error_type}: Close Connection");
                    return Ok(());
                }
                Err(UserPreambleError::IO(e)) => {
                    println!("[{current_connection}] Error {e}: Close Connection");
                    return Err(e);
                }
            }

            // Handle message interchange for connected user client
            chat_room_client.process_message_interchange().await?;

            // User disconnected -> Broadcast exit message
            println!("[{current_connection}] User disconnected");
            chat_room_client.broadcast_disconnect_message();
            Ok(())
        });
    }
}

fn try_join_with_user_name(
    new_user_name: &String,
    participant_user_names: Arc<Mutex<HashSet<String>>>,
) -> Result<String, ()> {
    let mut names = participant_user_names.lock().unwrap();
    if names.is_empty() {
        // New user is first joining user -> Valid & Return empty participants list
        names.insert(new_user_name.clone());
        Ok(String::from("-"))
    } else if names.contains(new_user_name) {
        // Name is already in use -> Invalid
        Err(())
    } else {
        // New user has an unused name -> Valid & Return comma separated participants list
        let participants_list = names.iter().join(", ");
        names.insert(new_user_name.clone());
        Ok(participants_list)
    }
}

fn is_valid_name(name: &str) -> bool {
    (1..=16).contains(&name.len()) && name.chars().all(char::is_alphanumeric)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_name_check() {
        assert!(is_valid_name("a"));
        assert!(is_valid_name("1"));
        assert!(is_valid_name("abc123"));

        assert!(!is_valid_name(""));
        assert!(!is_valid_name(" "));
        assert!(!is_valid_name("-"));
        assert!(!is_valid_name("abc+123"));
        assert!(!is_valid_name("abcdefghijklmnopq"));
    }
}
