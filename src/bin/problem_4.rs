use futures::StreamExt;
use std::collections::HashMap;
use std::io::Result as IO_Result;
use std::sync::{Arc, Mutex};

use tokio::net::UdpSocket;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;

#[derive(Debug, PartialEq)]
enum QueryType {
    Insert(String, String),
    Retrieve(String),
    Version,
}

#[tokio::main]
async fn main() -> IO_Result<()> {
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:8080").await?);
    let mut udp_framed = UdpFramed::new(Arc::clone(&udp_socket), BytesCodec::new());

    println!("Running server for Problem 4 on port 8080");

    let db = Arc::new(Mutex::new(HashMap::new()));

    while let Some(client_request) = udp_framed.next().await {
        if let Ok((request_query, client_address)) = client_request {
            let socket = Arc::clone(&udp_socket);
            let db = Arc::clone(&db);

            tokio::spawn(async move {
                let query_string = String::from_utf8(request_query.freeze().to_vec()).unwrap();
                let query_string = query_string.trim();

                println!("[{client_address}] Request: {query_string}");

                let query_result = execute_query(parse_query(query_string), db);
                if let Some(query_result_string) = query_result {
                    println!("[{client_address}] Response: {query_result_string}");
                    socket
                        .send_to(query_result_string.as_bytes(), client_address)
                        .await
                        .unwrap_or_else(|_| {
                            panic!("[{client_address}] Unable to send response to client")
                        });
                }
            });
        }
    }

    Ok(())
}

fn execute_query(
    query: QueryType,
    key_value_db: Arc<Mutex<HashMap<String, String>>>,
) -> Option<String> {
    let mut key_value_db_state = key_value_db.lock().unwrap();

    match query {
        QueryType::Version => {
            println!("[DB] Retrieving version");
            Some(String::from("version=Key-Value Store API v1"))
        }
        QueryType::Retrieve(key) => {
            println!("[DB] Retrieving value for {key}");
            key_value_db_state
                .get(&key)
                .map(|value| format!("{key}={value}"))
        }
        QueryType::Insert(key, value) => {
            println!("[DB] Inserting {key}->{value}");
            key_value_db_state.insert(key, value);
            None
        }
    }
}

fn parse_query(query_string: &str) -> QueryType {
    if query_string == "version" {
        QueryType::Version
    } else if let Some((key, value)) = query_string.split_once('=') {
        QueryType::Insert(String::from(key), String::from(value))
    } else {
        QueryType::Retrieve(String::from(query_string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_parsing() {
        // Version query
        assert_eq!(parse_query("version"), QueryType::Version);

        // Retrieve query
        assert_eq!(parse_query("foo"), QueryType::Retrieve(String::from("foo")));
        assert_eq!(
            parse_query("foo_bar"),
            QueryType::Retrieve(String::from("foo_bar"))
        );

        // Insert Query
        assert_eq!(
            parse_query("foo=bar"),
            QueryType::Insert(String::from("foo"), String::from("bar"))
        );
        assert_eq!(
            parse_query("foo=bar=baz"),
            QueryType::Insert(String::from("foo"), String::from("bar=baz"))
        );
        assert_eq!(
            parse_query("foo="),
            QueryType::Insert(String::from("foo"), String::new())
        );
        assert_eq!(
            parse_query("foo==="),
            QueryType::Insert(String::from("foo"), String::from("=="))
        );
        assert_eq!(
            parse_query("=foo"),
            QueryType::Insert(String::new(), String::from("foo"))
        );
    }

    #[test]
    fn test_version_query_execution() {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let version_query = QueryType::Version;

        assert_eq!(
            execute_query(version_query, Arc::clone(&db)),
            Some(String::from("version=Key-Value Store API v1"))
        );
        assert!(db.lock().unwrap().is_empty());
    }

    #[test]
    fn test_retrieve_query_execution() {
        let db = Arc::new(Mutex::new(HashMap::from([(
            String::from("abc"),
            String::from("42"),
        )])));
        let retrieve_abc_query = QueryType::Retrieve(String::from("abc"));
        let retrieve_def_query = QueryType::Retrieve(String::from("def"));

        assert_eq!(
            execute_query(retrieve_abc_query, Arc::clone(&db)),
            Some(String::from("abc=42"))
        );
        assert_eq!(execute_query(retrieve_def_query, Arc::clone(&db)), None);
        assert_eq!(
            *db.lock().unwrap(),
            HashMap::from([(String::from("abc"), String::from("42"),)])
        );
    }

    #[test]
    fn test_insert_query_execution() {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let insert_value_query = QueryType::Insert(String::from("abc"), String::from("42"));

        assert_eq!(execute_query(insert_value_query, Arc::clone(&db)), None);
        assert_eq!(
            *db.lock().unwrap(),
            HashMap::from([(String::from("abc"), String::from("42"),)])
        );

        let update_value_query = QueryType::Insert(String::from("abc"), String::from("123"));

        assert_eq!(execute_query(update_value_query, Arc::clone(&db)), None);
        assert_eq!(
            *db.lock().unwrap(),
            HashMap::from([(String::from("abc"), String::from("123"),)])
        );
    }
}
