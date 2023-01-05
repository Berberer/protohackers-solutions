use std::io::Result as IO_Result;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;

#[derive(Deserialize)]
struct MethodRequest {
    method: String,
    number: f64,
}

#[derive(Serialize)]
struct MethodResponse {
    method: String,
    prime: bool,
}

#[tokio::main]
async fn main() -> IO_Result<()> {
    let tcp_listener = TcpListener::bind("0.0.0.0:8080").await?;

    println!("Running server for Problem 1 on port 8080");

    let mut conn_counter = 1;

    loop {
        let (tcp_socket_stream, client_address) = tcp_listener.accept().await?;
        let current_connection = conn_counter;
        conn_counter += 1;
        println!(
            "Established connection {} from {:?}",
            current_connection, client_address
        );

        let (mut tcp_socket_reader, mut tcp_socket_writer) = tcp_socket_stream.into_split();

        tokio::spawn(async move {
            let task_result = check_prime(&mut tcp_socket_reader, &mut tcp_socket_writer).await;
            println!(
                "Closed connection {} to {:?}",
                current_connection, client_address
            );
            task_result
        });
    }
}

async fn check_prime(reader: &mut OwnedReadHalf, writer: &mut OwnedWriteHalf) -> IO_Result<()> {
    let reader = BufReader::new(reader);
    let mut request_lines = reader.lines();

    let mut writer = BufWriter::new(writer);

    while let Some(request_line) = request_lines.next_line().await? {
        if let Some(n) = parse_request(&request_line) {
            // Request in this lines was valid -> Respond prime check result
            let response = MethodResponse {
                method: String::from("isPrime"),
                prime: is_prime(n),
            };
            let response_json = serde_json::to_string(&response)?;

            println!("Request {} -> {}", request_line, response_json);

            writer
                .write_all(format!("{}\n", response_json).as_bytes())
                .await?;
            writer.flush().await?;
        } else {
            // Request in this line was malformed -> Respond with malformed response and stop
            println!("Request {} was malformed", request_line);

            writer.write_all(b"{}").await?;
            writer.flush().await?;
            break;
        }
    }

    Ok(())
}

fn parse_request(request_payload: &str) -> Option<f64> {
    let request: Result<MethodRequest, serde_json::Error> = serde_json::from_str(request_payload);

    if let Ok(MethodRequest { method, number }) = request {
        if method == "isPrime" {
            Some(number)
        } else {
            None
        }
    } else {
        None
    }
}

fn is_prime(n: f64) -> bool {
    if n > 1.0 && n.trunc() == n {
        // n is integer -> Check if prime number
        let n_int = n.trunc() as i64;

        if n_int <= 3 {
            return true;
        }

        for i in 2..=(n.sqrt() as i64) {
            if n_int % i == 0 {
                return false;
            }
        }

        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_parsing() {
        assert_eq!(parse_request(""), None);
        assert_eq!(parse_request(" abc "), None);
        assert_eq!(parse_request(r#"{"number": 1}"#), None);
        assert_eq!(parse_request(r#"{"method": "isPrime"}"#), None);
        assert_eq!(
            parse_request(r#"{"method": "isPrime", "number": "abc"}"#),
            None
        );
        assert_eq!(
            parse_request(r#"{"method": "isPrime", "number": 1}"#),
            Some(1.0)
        );
        assert_eq!(
            parse_request(r#"{"method": "isPrime", "number": 2.3}"#),
            Some(2.3)
        );
        assert_eq!(
            parse_request(r#"{"method": "isPrime", "number": 0, "anotherKey": true}"#),
            Some(0.0)
        );
    }

    #[test]
    fn test_prime_check() {
        assert!(!is_prime(-1.0));
        assert!(!is_prime(0.0));
        assert!(!is_prime(1.0));
        assert!(!is_prime(1.2345));

        assert!(is_prime(2.0));
        assert!(is_prime(3.0));
        assert!(!is_prime(4.0));
        assert!(is_prime(5.0));
        assert!(!is_prime(6.0));
        assert!(is_prime(7.0));
        assert!(!is_prime(8.0));
        assert!(!is_prime(9.0));
        assert!(!is_prime(10.0));
    }
}
