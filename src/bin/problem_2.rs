use std::io::Result as IO_Result;
use std::str;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;

#[derive(Debug, PartialEq)]
enum RequestType {
    Insert(i32, i32),
    Query(i32, i32),
}

#[tokio::main]
async fn main() -> IO_Result<()> {
    let tcp_listener = TcpListener::bind("0.0.0.0:8080").await?;

    println!("Running server for Problem 2 on port 8080");

    let mut asset_id = 0;
    loop {
        let (tcp_socket_stream, _) = tcp_listener.accept().await?;

        let (mut tcp_socket_reader, mut tcp_socket_writer) = tcp_socket_stream.into_split();

        tokio::spawn(async move {
            handle_asset_requests(asset_id, &mut tcp_socket_reader, &mut tcp_socket_writer).await
        });

        asset_id += 1;
    }
}

async fn handle_asset_requests(
    asset_id: i32,
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
) -> IO_Result<()> {
    let mut current_request_buffer = [0_u8; 9];
    let mut asset_prices = Vec::new();

    while let Ok(9) = reader.read_exact(&mut current_request_buffer).await {
        match parse_request(&current_request_buffer) {
            Ok(RequestType::Insert(timestamp, price)) => {
                println!("[Asset {}] Insert for {}: {}", asset_id, timestamp, price);
                asset_prices.push((timestamp, price))
            }
            Ok(RequestType::Query(from_timestamp, to_timestamp)) => {
                let average =
                    calculate_asset_price_average(&asset_prices, from_timestamp, to_timestamp);
                println!(
                    "[Asset {}] Query average {}-{}: {}",
                    asset_id, from_timestamp, to_timestamp, average
                );
                writer.write_i32(average).await?;
                writer.flush().await?;
            }
            Err(error_description) => println!(
                "[Asset {}] Malformed request {}: {}",
                asset_id,
                str::from_utf8(&current_request_buffer).unwrap(),
                error_description
            ),
        }
    }

    Ok(())
}

fn parse_request(request_payload: &[u8]) -> Result<RequestType, String> {
    if request_payload.len() != 9 {
        return Err(String::from("Requests must have 9 bytes"));
    }

    let request_param_1 = parse_number(&request_payload[1..=4])?;
    let request_param_2 = parse_number(&request_payload[5..=8])?;

    match request_payload[0] {
        b'I' => Ok(RequestType::Insert(request_param_1, request_param_2)),
        b'Q' => Ok(RequestType::Query(request_param_1, request_param_2)),
        unknown_operation_specifier => Err(format!(
            "No operation specified for {}",
            char::from(unknown_operation_specifier)
        )),
    }
}

fn parse_number(number_bytes: &[u8]) -> Result<i32, String> {
    number_bytes
        .try_into()
        .map(i32::from_be_bytes)
        .map_err(|_| format!("Malformed number {:02X?}", number_bytes))
}

fn calculate_asset_price_average(
    prices: &[(i32, i32)],
    from_timestamp: i32,
    to_timestamp: i32,
) -> i32 {
    let prices_in_timeframe: Vec<i32> = prices
        .iter()
        .filter(|(timestamp, _)| from_timestamp <= *timestamp && *timestamp <= to_timestamp)
        .map(|(_, price)| *price)
        .collect();

    if prices_in_timeframe.is_empty() {
        0
    } else {
        let price_sum: i32 = prices_in_timeframe.iter().cloned().sum();
        price_sum / (prices_in_timeframe.len() as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_number_parsing() {
        assert_eq!(
            parse_number(&[0x00]),
            Err(String::from("Malformed number [00]"))
        );
        assert_eq!(
            parse_number(&[0x00, 0x01, 0x02, 0x03, 0x04, 0x05]),
            Err(String::from("Malformed number [00, 01, 02, 03, 04, 05]"))
        );
        assert_eq!(parse_number(&[0x00, 0x00, 0x00, 0x10]), Ok(16));
        assert_eq!(parse_number(&[0xFF, 0xFF, 0xFF, 0xF0]), Ok(-16));
    }

    #[test]
    fn test_request_parsing() {
        assert_eq!(
            parse_request(&[0x00]),
            Err(String::from("Requests must have 9 bytes"))
        );
        assert_eq!(
            parse_request(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            Err(String::from("Requests must have 9 bytes"))
        );
        assert_eq!(
            parse_request(&[0x00]),
            Err(String::from("Requests must have 9 bytes"))
        );
        assert_eq!(
            parse_request(&[0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            Err(String::from("No operation specified for A"))
        );

        assert_eq!(
            parse_request(&[0x49, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02]),
            Ok(RequestType::Insert(1, 2))
        );
        assert_eq!(
            parse_request(&[0x51, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04]),
            Ok(RequestType::Query(3, 4))
        );
    }

    #[test]
    fn test_average_calculation() {
        let prices = vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)];
        assert_eq!(calculate_asset_price_average(&prices, 10, 11), 0);
        assert_eq!(calculate_asset_price_average(&prices, 1, 3), 3);
    }
}
