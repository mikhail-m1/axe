use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use futures_util::Stream;
use log::debug;
use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("reqwest error")]
    Rewqest(#[from] reqwest::Error),
    #[error("UTF-8 error")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("JSON Parser error")]
    Json(#[from] serde_json::Error),
    #[error("Unexpected Stream header type {0}")]
    UnsupportedStreamHeaderType(u8),
    #[error("Unexpected Stream header content type {0}")]
    UnexpectedStreamContentType(String),
    #[error("Failed to parse Stream")]
    StreamParse(String),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SessionUpdate {
    pub session_metadata: SessionMetadata,
    pub session_results: Vec<SessionResult>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SessionMetadata {
    pub sampled: bool,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SessionResult {
    pub ingestion_time: u64,
    pub log_group_identifier: String,
    pub log_stream_name: String,
    pub message: String,
    pub timestamp: u64,
}

pub struct MessageParser<Input>
where
    Input: Stream<Item = Result<String, Error>>,
{
    input: std::pin::Pin<Box<Input>>,
    parsed: VecDeque<SessionResult>,
}

impl<Input> MessageParser<Input>
where
    Input: Stream<Item = Result<String, Error>>,
{
    pub fn new(input: Input) -> Self {
        Self {
            input: Box::pin(input),
            parsed: VecDeque::new(),
        }
    }
}

impl<Input> futures_core::Stream for MessageParser<Input>
where
    Input: Stream<Item = Result<String, Error>>,
{
    type Item = Result<SessionResult, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(message) = self.parsed.pop_front() {
                return Poll::Ready(Some(Ok(message)));
            }
            return match futures_core::ready!(self.input.as_mut().poll_next(cx)) {
                Some(Ok(messages)) => match serde_json::from_str::<SessionUpdate>(&messages) {
                    Err(error) => return Poll::Ready(Some(Err(Error::Json(error)))),
                    Ok(update) => {
                        self.parsed = VecDeque::from(update.session_results);
                        debug!("parsed {} messages", self.parsed.len());
                        continue;
                    }
                },
                Some(Err(err)) => Poll::Ready(Some(Err(err))),
                None => Poll::Ready(None),
            };
        }
    }
}

pub struct EventStreamParser<Input>
where
    Input: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    input: std::pin::Pin<Box<Input>>,
    buffer: Option<Bytes>,
    state: ParserState,
    remain_len: u32,
    previous: VecDeque<u8>,
    header_name: Option<String>,
    is_session_update: bool,
}

#[derive(Debug)]
enum ParserState {
    BeforeLength,
    BeforeHeaderLength,
    BeforePreludeCRC { header_len: u32 },
    Header { len: u32, state: HeaderParserState },
    Message,
    BeforeCRC,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy)]
enum HeaderParserState {
    BeforeNameLength,
    BeforeName(u8),
    BeforeValueType,
    BeforeValueLenght,
    BeforeValue(u16),
}

impl HeaderParserState {
    fn to_state(self, remain_header_len: u32) -> ParserState {
        ParserState::Header {
            len: remain_header_len,
            state: self,
        }
    }
}

impl<Input> EventStreamParser<Input>
where
    Input: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    pub fn new(input: Input) -> Self {
        Self {
            input: Box::pin(input),
            state: ParserState::BeforeLength,
            buffer: None,
            remain_len: 0,
            previous: VecDeque::new(),
            header_name: None,
            is_session_update: false,
        }
    }

    fn add(&mut self, buffer: Bytes) {
        if let Some(previous_buffer) = &self.buffer {
            self.previous.extend(previous_buffer.iter());
        }
        self.buffer = Some(buffer);
        debug!("prev {:?} buffer {:?}", self.previous, self.buffer)
    }

    // Parser for subset of https://docs.aws.amazon.com/transcribe/latest/dg/streaming-setting-up.html
    fn get(&mut self) -> Result<Option<String>, Error> {
        loop {
            let keep_going = match self.state {
                ParserState::BeforeLength => {
                    self.remain_len = u32::MAX;
                    self.read_u32().process_some(|v| {
                        self.remain_len = v - 4;
                        self.state = ParserState::BeforeHeaderLength
                    })?
                }
                ParserState::BeforeHeaderLength => self.read_u32().process_some(|header_len| {
                    self.state = ParserState::BeforePreludeCRC { header_len }
                })?,
                ParserState::BeforePreludeCRC { header_len } => {
                    self.read_u32().process_some(|_| {
                        self.state = ParserState::Header {
                            len: header_len,
                            state: HeaderParserState::BeforeNameLength,
                        }
                    })?
                }
                ParserState::Header { len, ref state } => self.read_header(len, *state)?,
                ParserState::Message => {
                    if let Some(message) = self.read_string(self.remain_len - 4)? {
                        self.state = ParserState::BeforeCRC;
                        if self.is_session_update {
                            return Ok(Some(message));
                        }
                    }
                    false
                }
                ParserState::BeforeCRC => self.read_u32().process_some_err(|_| {
                    if self.remain_len != 0 {
                        return Err(Error::StreamParse(format!(
                            "remain size {} has to be 0",
                            self.remain_len
                        )));
                    }
                    if !self.previous.is_empty() {
                        return Err(Error::StreamParse("expected previous to be empty".into()));
                    }
                    self.state = ParserState::BeforeLength;
                    self.is_session_update = false;
                    Ok(())
                })?,
            };
            if !keep_going {
                return Ok(None);
            }
            debug!(
                "{:?} total: {:?} {:?} {:?}",
                self.state, self.remain_len, self.previous, self.buffer
            );
        }
    }

    fn read_header(&mut self, len: u32, state: HeaderParserState) -> Result<bool, Error> {
        if len == 0 {
            self.state = ParserState::Message;
            Ok(true)
        } else {
            match state {
                HeaderParserState::BeforeNameLength => self.read_u8().process_some(|v| {
                    self.state = HeaderParserState::BeforeName(v).to_state(len - 1);
                }),
                HeaderParserState::BeforeName(str_len) => {
                    self.read_string(str_len as u32)?.process_some(|v| {
                        self.header_name = Some(v);
                        self.state =
                            HeaderParserState::BeforeValueType.to_state(len - str_len as u32);
                    })
                }
                HeaderParserState::BeforeValueType => self.read_u8().process_some_err(|v| {
                    if v != 7 {
                        // Only String type is supported
                        Err(Error::UnsupportedStreamHeaderType(v))
                    } else {
                        self.state = HeaderParserState::BeforeValueLenght.to_state(len - 1);
                        Ok(())
                    }
                }),
                HeaderParserState::BeforeValueLenght => self.read_u16().process_some(|v| {
                    self.state = HeaderParserState::BeforeValue(v).to_state(len - 2);
                }),
                HeaderParserState::BeforeValue(str_len) => {
                    self.read_string(str_len as u32)?.process_some_err(|value| {
                        if self.header_name.is_none() {
                            return Err(Error::StreamParse("Header name is missing".into()));
                        }
                        let name = self.header_name.take().unwrap();
                        if name == ":content-type" && value != "application/x-amz-json-1.1" {
                            return Err(Error::UnexpectedStreamContentType(value));
                        }
                        if name == ":event-type" && value == "sessionUpdate" {
                            self.is_session_update = true;
                        }
                        self.state =
                            HeaderParserState::BeforeNameLength.to_state(len - str_len as u32);
                        Ok(())
                    })
                }
            }
        }
    }

    fn read_u32(&mut self) -> Option<u32> {
        self.read(4)
            .map(|it| it.fold(0, |a, v| a << 8 | v as u32))
            .inspect(|_| self.advance(4))
    }

    fn read_u16(&mut self) -> Option<u16> {
        self.read(2)
            .map(|it| it.fold(0, |a, v| a << 8 | v as u16))
            .inspect(|_| self.advance(2))
    }

    fn read_u8(&mut self) -> Option<u8> {
        self.read(1)
            .and_then(|mut it| it.next())
            .inspect(|_| self.advance(1))
    }

    fn read_string(&mut self, len: u32) -> Result<Option<String>, Error> {
        // TODO: optimize mem allocs
        Ok(self
            .read(len)
            .map(|it| it.collect::<Vec<_>>())
            .inspect(|_| self.advance(len)))
        .and_then(|v| {
            if let Some(v) = v {
                Ok(Some(String::from_utf8(v).map_err(Error::Utf8)?))
            } else {
                Ok(None)
            }
        })
    }

    fn read(&mut self, len: u32) -> Option<impl Iterator<Item = u8> + use<'_, Input>> {
        if self.buffer.as_ref().map(|v| v.len()).unwrap_or(0) + self.previous.len() < len as usize {
            None
        } else {
            Some(
                self.previous
                    .iter()
                    .chain(self.buffer.as_ref().unwrap().iter())
                    .take(len as usize)
                    .copied(),
            )
        }
    }

    fn advance(&mut self, len: u32) {
        let from_buffer = (len as usize).saturating_sub(self.previous.len());
        for _ in 0..len {
            self.previous.pop_front();
        }
        self.buffer.as_mut().unwrap().advance(from_buffer);
        self.remain_len -= len;
    }
}

impl<Input> Stream for EventStreamParser<Input>
where
    Input: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    type Item = Result<String, Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let self_mut = self.get_mut();
        loop {
            match self_mut.get() {
                Ok(Some(message)) => return Poll::Ready(Some(Ok(message))),
                Err(e) => return Poll::Ready(Some(Err(e))),
                _ => {}
            }
            return match futures_core::ready!(self_mut.input.as_mut().poll_next(cx)) {
                Some(Ok(bytes)) => {
                    self_mut.add(bytes);
                    continue;
                }
                Some(Err(err)) => Poll::Ready(Some(Err(Error::Rewqest(err)))),
                None => Poll::Ready(None),
            };
        }
    }
}

trait OptionExt<T> {
    fn process_some(self, f: impl FnOnce(T)) -> Result<bool, Error>;

    fn process_some_err(self, f: impl FnOnce(T) -> Result<(), Error>) -> Result<bool, Error>;
}

impl<T> OptionExt<T> for Option<T> {
    fn process_some(self, f: impl FnOnce(T)) -> Result<bool, Error> {
        if let Some(v) = self {
            f(v);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn process_some_err(self, f: impl FnOnce(T) -> Result<(), Error>) -> Result<bool, Error> {
        match self {
            Some(v) => {
                f(v)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

//FIXME: convert to test
#[cfg(test)]
mod test {
    use super::*;
    use futures_util::StreamExt;
    use futures_util::{pin_mut, stream};

    #[tokio::test]
    async fn event_stream_parser() {
        // checksums are invalid
        let input = b"\0\0\0\xa6\0\0\0]\x8f\x9f\x98\x16\x0b:event-type\x07\0\rsessionUpdate\r:content-type\x07\0\x1aapplication/x-amz-json-1.1\r:message-type\x07\0\x05event{\"sessionMetadata\":{\"sampled\":false},\"sessionResults\":[]}v\x0f\x8aw";
        let stream = stream::iter(input).map(|v| Ok(Bytes::from_owner([*v])));
        let parser = EventStreamParser::new(stream);
        pin_mut!(parser);
        assert_eq!(
            parser.next().await.unwrap().unwrap(),
            "{\"sessionMetadata\":{\"sampled\":false},\"sessionResults\":[]}"
        );
        assert!(parser.next().await.is_none());
    }

    #[tokio::test]
    async fn event_stream_and_message() {
        // checksums are invalid
        let input = b"\0\0\x01\x75\0\0\0]\x8f\x9f\x98\x16\x0b:event-type\x07\0\rsessionUpdate\r:content-type\x07\0\x1aapplication/x-amz-json-1.1\r:message-type\x07\0\x05event{\"sessionMetadata\":{\"sampled\":false},\"sessionResults\":[{\"ingestionTime\":1,\"logGroupIdentifier\":\"group\",\"logStreamName\":\"stream\",\"message\":\"msg\",\"timestamp\":2},{\"ingestionTime\":3,\"logGroupIdentifier\":\"group\",\"logStreamName\":\"stream\",\"message\":\"ms2\",\"timestamp\":4}]}v\x0f\x8aw\0\0\0\xa6\0\0\0]\x8f\x9f\x98\x16\x0b:event-type\x07\0\rsessionUpdate\r:content-type\x07\0\x1aapplication/x-amz-json-1.1\r:message-type\x07\0\x05event{\"sessionMetadata\":{\"sampled\":false},\"sessionResults\":[]}v\x0f\x8aw";
        let stream = stream::iter(input).map(|v| Ok(Bytes::from_owner([*v])));
        let parser = MessageParser::new(EventStreamParser::new(stream));
        pin_mut!(parser);
        let sr = parser.next().await.unwrap().unwrap();
        assert_eq!(sr.ingestion_time, 1);
        assert_eq!(sr.log_group_identifier, "group");
        assert_eq!(sr.log_stream_name, "stream");
        assert_eq!(sr.message, "msg");
        assert_eq!(sr.timestamp, 2);
        let sr = parser.next().await.unwrap().unwrap();
        assert_eq!(sr.ingestion_time, 3);
        assert_eq!(sr.log_group_identifier, "group");
        assert_eq!(sr.log_stream_name, "stream");
        assert_eq!(sr.message, "ms2");
        assert_eq!(sr.timestamp, 4);
        assert!(parser.next().await.is_none());
    }

    #[test]
    fn json_deserialize() {
        let su = serde_json::from_str::<SessionUpdate>("{\"sessionMetadata\":{\"sampled\":false},\"sessionResults\":[{\"ingestionTime\":10,\"logGroupIdentifier\":\"group\",\"logStreamName\":\"stream\",\"message\":\"2024-12-28 msg\",\"timestamp\":42}]}");
        assert_eq!(
            su.unwrap().session_results.first().unwrap().message,
            "2024-12-28 msg"
        );
    }
}
