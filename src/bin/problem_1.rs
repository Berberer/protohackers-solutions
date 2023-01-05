use std::io::Result as IO_Result;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

    loop {
        let (tcp_socket_stream, _) = tcp_listener.accept().await?;
        let (mut tcp_socket_reader, mut tcp_socket_writer) = tcp_socket_stream.into_split();
        check_prime(&mut tcp_socket_reader, &mut tcp_socket_writer).await?;
    }
}

async fn check_prime(reader: &mut OwnedReadHalf, writer: &mut OwnedWriteHalf) -> IO_Result<()> {
    let mut payload_buffer = Vec::new();
    reader.read_to_end(&mut payload_buffer).await?;

    if let Ok(payload_string) = String::from_utf8(payload_buffer) {
        let requests: Vec<Option<f64>> = payload_string.lines().map(parse_request).collect();

        let mut response_strings: Vec<String> = Vec::new();

        for request in requests {
            if let Some(n) = request {
                // Request in this lines was valid -> Respond prime check result
                let response = MethodResponse {
                    method: String::from("isPrime"),
                    prime: is_prime(n),
                };

                let response_json = serde_json::to_string(&response)?;
                response_strings.push(response_json);
            } else {
                // Request in this line was malformed -> Respond with malformed response
                response_strings.push(String::from("{}"));
            }
        }

        writer
            .write_all(response_strings.join("\n").as_bytes())
            .await?;
    } else {
        // Request is not valid UTF-8 -> Respond with malformed response
        writer.write_all(b"{}").await?;
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
    if n.trunc() == n {
        // n is integer -> Check if prime number
        let n_int = n.trunc() as i64;
        return if n_int <= 3 {
            true
        } else {
            for i in 2..=(n.sqrt() as i64) {
                if n_int % i == 0 {
                    return false;
                }
            }
            true
        };
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
        assert!(!is_prime(1.2345));
        assert!(is_prime(0.0));
        assert!(is_prime(1.0));
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
