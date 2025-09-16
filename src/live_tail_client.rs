use futures_util::StreamExt;
use hmac::digest::InvalidLength;
use log::debug;
use reqwest::header::HeaderValue;
use serde::Serialize;

use crate::live_tail_parser::{Error as ParserError, EventStreamParser, MessageParser};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Signing failed: {0}")]
    Signing(InvalidLength),
    #[error("Unexpected HTTP header content type: {0}")]
    UnexpectedHttpContentType(String),
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),
    #[error("HTTP request header error")]
    HttpHeader,
    #[error("API error: {0}")]
    Api(String),
    #[error("HTTP request header error: {0}")]
    Parser(ParserError),
    #[error("Serialize error: {0}")]
    Serialize(serde_json::Error),
}

// Own implementation of StartLiveTail request, because the SDK doesn't support it
pub async fn request_and_process(
    credentials: &aws_credential_types::Credentials,
    region: &str,
    group_arn: &str,
    stream: Option<&str>,
    filter: Option<&str>,
    mut consumer: impl FnMut(Option<i64>, Option<String>) -> bool,
) -> Result<(), Error> {
    let log_stream_names = if let Some(stream) = stream {
        vec![stream.to_string()]
    } else {
        vec![]
    };
    let body = serde_json::to_string(&LiveTailRequest {
        log_event_filter_pattern: filter.unwrap_or_default().to_string(),
        log_group_identifiers: vec![group_arn.to_string()],
        log_stream_names,
    })
    .map_err(Error::Serialize)?;
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let now_string = now.format("%Y%m%dT%H%M%SZ").to_string();
    let secret = format!("AWS4{}", credentials.secret_access_key());
    let secret = sign(secret.as_bytes(), &date)?;
    let secret = sign(&secret, region)?;
    let secret = sign(&secret, "logs")?;
    let final_signing_key = sign(&secret, "aws4_request")?;

    let mut hash = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(&mut hash, &body);
    let body_hash = format!("{:x}", sha2::Digest::finalize(hash));
    let (sign_headers, token_header) = if let Some(token) = credentials.session_token() {
        (
            "content-type;host;x-amz-date;x-amz-security-token;x-amz-target",
            format!("\nx-amz-security-token:{token}"),
        )
    } else {
        ("content-type;host;x-amz-date;x-amz-target", String::new())
    };
    let request_to_sign = format!(
        r#"POST
/

content-type:application/x-amz-json-1.1
host:streaming-logs.{region}.amazonaws.com
x-amz-date:{now_string}{token_header}
x-amz-target:Logs_20140328.StartLiveTail

{sign_headers}
"#
    );

    let mut hash = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(&mut hash, &request_to_sign);
    sha2::Digest::update(&mut hash, &body_hash);
    let request_hash = format!("{:x}", sha2::Digest::finalize(hash));

    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{now_string}\n{date}/{region}/logs/aws4_request\n{request_hash}"
    );
    let final_signature = sign_to_str(&final_signing_key, &to_sign)?;

    let mut headers = reqwest::header::HeaderMap::new();
    headers.append(
        "Content-Type",
        HeaderValue::from_static("application/x-amz-json-1.1"),
    );
    headers.append(
        "User-Agent",
        HeaderValue::from_str(&format!(
            "cw-axe/{}",
            option_env!("CARGO_PKG_VERSION").unwrap_or_default(),
        ))
        .map_err(|_| Error::HttpHeader)?,
    );
    headers.append(
        "X-Amz-Target",
        HeaderValue::from_static("Logs_20140328.StartLiveTail"),
    );
    headers.append(
        "X-Amz-Date",
        HeaderValue::from_str(&now_string).map_err(|_| Error::HttpHeader)?,
    );
    if let Some(token) = credentials.session_token() {
        headers.append(
            "X-Amz-Security-Token",
            HeaderValue::from_str(token).map_err(|_| Error::HttpHeader)?,
        );
    }
    let auth = format!("AWS4-HMAC-SHA256 Credential={}/{date}/{region}/logs/aws4_request, SignedHeaders={sign_headers}, Signature={final_signature}", credentials.access_key_id());
    headers.append(
        "Authorization",
        HeaderValue::from_str(&auth).map_err(|_| Error::HttpHeader)?,
    );

    debug!("POST headers {headers:?} body {body}");
    let response = reqwest::Client::new()
        .post(format!("https://streaming-logs.{region}.amazonaws.com"))
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(Error::Http)?;

    let resonse_headers = response.headers();
    let content_type = resonse_headers.get("Content-Type");

    // JSON response means something went wrong
    if content_type
        .map(|v| v == "application/x-amz-json-1.1")
        .unwrap_or(false)
    {
        return Err(Error::Api(response.text().await.map_err(Error::Http)?));
    }

    if content_type
        .map(|v| v != "application/vnd.amazon.eventstream")
        .unwrap_or(true)
    {
        return Err(Error::UnexpectedHttpContentType(format!(
            "{content_type:?}"
        )));
    }
    debug!("{resonse_headers:?}");
    let parser = MessageParser::new(EventStreamParser::new(response.bytes_stream()));
    futures_util::pin_mut!(parser);
    while let Some(event) = parser.next().await {
        let message = event.map_err(Error::Parser)?;
        if !(consumer(Some(message.ingestion_time as i64), Some(message.message))) {
            break;
        }
    }
    Ok(())
}

fn sign(key: &[u8], content: &str) -> Result<Vec<u8>, Error> {
    let mut h = <hmac::Hmac<sha2::Sha256> as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(Error::Signing)?;
    hmac::digest::Update::update(&mut h, content.as_bytes());
    Ok(hmac::Mac::finalize(h).into_bytes().to_vec())
}

fn sign_to_str(key: &[u8], content: &str) -> Result<String, Error> {
    let mut h = <hmac::Hmac<sha2::Sha256> as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(Error::Signing)?;
    hmac::digest::Update::update(&mut h, content.as_bytes());
    Ok(format!("{:x}", hmac::Mac::finalize(h).into_bytes()))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct LiveTailRequest {
    log_event_filter_pattern: String,
    log_group_identifiers: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    log_stream_names: Vec<String>,
}
