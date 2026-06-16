//! Minimal HTTP client for the example CLI.
//!
//! The server itself uses Axum through `rakka::http`. This client deliberately
//! stays dependency-free so the example does not add an HTTP client crate just
//! for README smoke tests.

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::model::CounterValue;
use crate::support::{example_error, ExampleResult};

pub async fn get_counter_json(endpoint: &str, path: &str) -> ExampleResult<CounterValue> {
    let (host, port) = parse_http_endpoint(endpoint)?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    read_counter_response(stream).await
}

pub async fn post_counter_json<T>(
    endpoint: &str,
    path: &str,
    payload: &T,
) -> ExampleResult<CounterValue>
where
    T: Serialize,
{
    let (host, port) = parse_http_endpoint(endpoint)?;
    let body = serde_json::to_vec(payload)?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nAccept: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    read_counter_response(stream).await
}

async fn read_counter_response(mut stream: TcpStream) -> ExampleResult<CounterValue> {
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let (status, body) = parse_http_response(&response)?;
    if !(200..300).contains(&status) {
        return Err(example_error(format!(
            "HTTP request failed with {status}: {}",
            String::from_utf8_lossy(&body)
        ))
        .into());
    }
    Ok(serde_json::from_slice(&body)?)
}

fn parse_http_endpoint(endpoint: &str) -> ExampleResult<(String, u16)> {
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| example_error("RAKKA_HTTP_ENDPOINT must start with http://"))?;
    let authority = authority.trim_end_matches('/');
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| example_error("RAKKA_HTTP_ENDPOINT must include host:port"))?;
    Ok((host.to_string(), port.parse()?))
}

fn parse_http_response(response: &[u8]) -> ExampleResult<(u16, Vec<u8>)> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(example_error("HTTP response did not include headers").into());
    };
    let header_bytes = &response[..header_end];
    let body = response[header_end + 4..].to_vec();
    let headers = std::str::from_utf8(header_bytes)?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| example_error("HTTP response status line was invalid"))?
        .parse::<u16>()?;

    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        return Ok((status, decode_chunked_body(&body)?));
    }

    Ok((status, body))
}

fn decode_chunked_body(body: &[u8]) -> ExampleResult<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut offset = 0usize;
    loop {
        let Some(line_end) = body[offset..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|position| offset + position)
        else {
            return Err(example_error("chunked response was truncated").into());
        };
        let size_text = std::str::from_utf8(&body[offset..line_end])?;
        let size = usize::from_str_radix(size_text.trim(), 16)?;
        offset = line_end + 2;
        if size == 0 {
            break;
        }
        let chunk_end = offset
            .checked_add(size)
            .ok_or_else(|| example_error("chunked response size overflow"))?;
        if body.len() < chunk_end + 2 {
            return Err(example_error("chunked response body was truncated").into());
        }
        decoded.extend_from_slice(&body[offset..chunk_end]);
        offset = chunk_end + 2;
    }
    Ok(decoded)
}
