use billing::model::TokenUsage;
use router::ports::UsageMeter;
use serde_json::Value;

use crate::error::MeterError;
use crate::sse::EventSplitter;
use crate::usage::{check_error, check_service_tier, token_usage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiMeter {
    mode: Mode,
    failure: Option<MeterError>,
    limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Stream(StreamState),
    Whole(Vec<u8>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamState {
    splitter: EventSplitter,
    done: bool,
    usage: Option<TokenUsage>,
}

impl OpenAiMeter {
    pub(crate) fn new(streaming: bool) -> Self {
        let mode = if streaming {
            Mode::Stream(StreamState::default())
        } else {
            Mode::Whole(Vec::new())
        };
        Self {
            mode,
            failure: None,
            limit: crate::MAX_BUFFER_BYTES,
        }
    }
}

impl StreamState {
    fn feed(&mut self, bytes: &[u8], failure: &mut Option<MeterError>) {
        if matches!(*failure, Some(MeterError::BufferLimitExceeded)) {
            return;
        }
        for event in self.splitter.push(bytes) {
            let outcome = self.event(&event, failure.is_some());
            if let (Err(error), None) = (outcome, *failure) {
                *failure = Some(error);
            }
        }
        if self.splitter.overflowed() && failure.is_none() {
            *failure = Some(MeterError::BufferLimitExceeded);
        }
    }

    fn event(&mut self, data: &[u8], failed: bool) -> Result<(), MeterError> {
        if self.done {
            return Err(MeterError::DataAfterDone);
        }
        if data == b"[DONE]" {
            self.done = true;
            return Ok(());
        }
        if failed {
            return Ok(());
        }
        let Ok(Value::Object(chunk)) = serde_json::from_slice::<Value>(data) else {
            return Err(MeterError::InvalidEvent);
        };
        check_error(&chunk)?;
        check_service_tier(&chunk)?;
        match chunk.get("usage") {
            None | Some(Value::Null) => Ok(()),
            Some(_) if self.usage.is_some() => Err(MeterError::DuplicateUsage),
            Some(value) => {
                self.usage = Some(token_usage(value)?);
                Ok(())
            }
        }
    }
}

fn whole_usage(body: &[u8]) -> Result<TokenUsage, MeterError> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Err(MeterError::IncompleteBody);
    };
    let Value::Object(object) = value else {
        return Err(MeterError::InvalidEvent);
    };
    check_error(&object)?;
    check_service_tier(&object)?;
    match object.get("usage") {
        None | Some(Value::Null) => Err(MeterError::MissingUsage),
        Some(usage) => token_usage(usage),
    }
}

fn feed_whole(body: &mut Vec<u8>, bytes: &[u8], limit: usize, failure: &mut Option<MeterError>) {
    if matches!(*failure, Some(MeterError::BufferLimitExceeded)) {
        return;
    }
    if body.len().saturating_add(bytes.len()) > limit {
        *body = Vec::new();
        *failure = Some(MeterError::BufferLimitExceeded);
        return;
    }
    body.extend_from_slice(bytes);
}

impl UsageMeter for OpenAiMeter {
    type Error = MeterError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), MeterError> {
        match &mut self.mode {
            Mode::Stream(state) => state.feed(bytes, &mut self.failure),
            Mode::Whole(body) => feed_whole(body, bytes, self.limit, &mut self.failure),
        }
        self.failure.map_or(Ok(()), Err)
    }

    fn ended(&self) -> bool {
        if matches!(self.failure, Some(MeterError::BufferLimitExceeded)) {
            return true;
        }
        match &self.mode {
            Mode::Stream(state) if state.splitter.overflowed() => true,
            Mode::Stream(state) => state.done,
            Mode::Whole(body) => serde_json::from_slice::<serde::de::IgnoredAny>(body).is_ok(),
        }
    }

    fn finish(self) -> Result<TokenUsage, MeterError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        match self.mode {
            Mode::Stream(state) => state.usage.ok_or(MeterError::MissingUsage),
            Mode::Whole(body) => whole_usage(&body),
        }
    }
}

#[cfg(test)]
impl OpenAiMeter {
    fn with_limit(streaming: bool, limit: usize) -> Self {
        let mode = if streaming {
            Mode::Stream(StreamState {
                splitter: crate::sse::EventSplitter::with_limit(limit),
                ..StreamState::default()
            })
        } else {
            Mode::Whole(Vec::new())
        };
        Self {
            mode,
            failure: None,
            limit,
        }
    }
}

const _: () = {
    const fn assert_auto_traits<
        T: Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
    >() {
    }
    assert_auto_traits::<OpenAiMeter>();
};

#[cfg(test)]
mod buffer_limit {
    use billing::model::TokenUsage;
    use router::ports::UsageMeter;

    use super::OpenAiMeter;
    use crate::error::MeterError;
    use crate::MAX_BUFFER_BYTES;

    const USAGE: &str = r#"{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20,"prompt_tokens_details":{"cached_tokens":3,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":2,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}}"#;

    const EXPECTED: TokenUsage = TokenUsage {
        input_tokens: 10,
        cached_input_tokens: 3,
        output_tokens: 7,
    };

    fn completion() -> Vec<u8> {
        format!(r#"{{"id":"c1","choices":[],"usage":{USAGE}}}"#).into_bytes()
    }

    fn usage_and_done() -> Vec<u8> {
        let event = format!(r#"data: {{"choices":[],"usage":{USAGE}}}"#);
        format!("{event}\n\ndata: [DONE]\n\n").into_bytes()
    }

    fn run(mut meter: OpenAiMeter, pieces: &[&[u8]]) -> (bool, Result<TokenUsage, MeterError>) {
        for piece in pieces {
            let _ = meter.feed(piece);
        }
        let ended = meter.ended();
        (ended, meter.finish())
    }

    #[test]
    fn production_limit_is_the_published_constant() {
        let whole = OpenAiMeter::new(false);
        let stream = OpenAiMeter::new(true);
        assert_eq!(whole.limit, MAX_BUFFER_BYTES);
        assert_eq!(stream.limit, MAX_BUFFER_BYTES);
        match &stream.mode {
            super::Mode::Stream(state) => {
                assert_eq!(state.splitter.limit(), MAX_BUFFER_BYTES);
            }
            super::Mode::Whole(_) => panic!("expected stream mode"),
        }
    }

    #[test]
    fn whole_body_at_the_limit_is_parsed() {
        let body = completion();
        let mut padded = body.clone();
        padded.resize(body.len() + 8, b' ');
        let limit = padded.len();
        let mut meter = OpenAiMeter::with_limit(false, limit);
        assert_eq!(meter.feed(&padded), Ok(()));
        assert!(meter.ended());
        assert_eq!(meter.finish(), Ok(EXPECTED));
    }

    #[test]
    fn whole_body_over_the_limit_is_not_truncated_and_parsed() {
        let body = completion();
        let limit = body.len();
        let mut over = body.clone();
        over.push(b' ');
        let mut meter = OpenAiMeter::with_limit(false, limit);
        assert_eq!(meter.feed(&over), Err(MeterError::BufferLimitExceeded));
        assert!(meter.ended());
        match &meter.mode {
            super::Mode::Whole(buf) => assert_eq!(buf.as_slice(), b"" as &[u8]),
            super::Mode::Stream(_) => panic!("expected whole mode"),
        }
        assert_eq!(meter.finish(), Err(MeterError::BufferLimitExceeded));
    }

    #[test]
    fn whole_body_overflow_on_a_later_chunk_does_not_parse_the_prefix() {
        let body = completion();
        let limit = body.len();
        let (head, tail) = body.split_at(body.len() - 1);
        let mut meter = OpenAiMeter::with_limit(false, limit);
        assert_eq!(meter.feed(head), Ok(()));
        assert!(!meter.ended());
        let mut last = tail.to_vec();
        last.push(b'x');
        assert_eq!(meter.feed(&last), Err(MeterError::BufferLimitExceeded));
        assert!(meter.ended());
        assert_eq!(meter.finish(), Err(MeterError::BufferLimitExceeded));
    }

    #[test]
    fn whole_body_stops_accumulating_after_overflow() {
        let mut meter = OpenAiMeter::with_limit(false, 4);
        assert_eq!(meter.feed(b"12345"), Err(MeterError::BufferLimitExceeded));
        assert_eq!(
            meter.feed(&completion()),
            Err(MeterError::BufferLimitExceeded)
        );
        assert!(meter.ended());
        assert_eq!(meter.finish(), Err(MeterError::BufferLimitExceeded));
    }

    #[test]
    fn stream_line_over_the_limit_is_not_parsed() {
        let mut line = vec![b'x'; 9];
        line.extend_from_slice(&usage_and_done());
        let mut same_feed = OpenAiMeter::with_limit(true, 8);
        assert_eq!(same_feed.feed(&line), Err(MeterError::BufferLimitExceeded));
        assert!(same_feed.ended());
        assert_eq!(same_feed.finish(), Err(MeterError::BufferLimitExceeded));
    }

    #[test]
    fn stream_stops_accumulating_after_overflow() {
        let mut meter = OpenAiMeter::with_limit(true, 8);
        assert_eq!(meter.feed(&[b'x'; 9]), Err(MeterError::BufferLimitExceeded));
        assert_eq!(
            meter.feed(&usage_and_done()),
            Err(MeterError::BufferLimitExceeded)
        );
        assert!(meter.ended());
        assert_eq!(meter.finish(), Err(MeterError::BufferLimitExceeded));
    }

    #[test]
    fn stream_line_at_the_limit_still_reads_usage() {
        let event = format!(r#"data: {{"choices":[],"usage":{USAGE}}}"#);
        let bytes = format!("{event}\n\ndata: [DONE]\n\n").into_bytes();
        let meter = OpenAiMeter::with_limit(true, event.len());
        assert_eq!(run(meter, &[&bytes]), (true, Ok(EXPECTED)));
    }

    #[test]
    fn stream_joined_data_at_the_limit_still_reads_usage() {
        let prefix = r#"{"choices":[],"#;
        let suffix = format!(r#""usage":{USAGE}}}"#);
        let joined_len = prefix.len() + 1 + suffix.len();
        let bytes = format!("data: {prefix}\ndata: {suffix}\n\ndata: [DONE]\n\n").into_bytes();
        let meter = OpenAiMeter::with_limit(true, joined_len);
        assert_eq!(run(meter, &[&bytes]), (true, Ok(EXPECTED)));
    }

    #[test]
    fn stream_under_the_limit_still_reads_usage() {
        let bytes = usage_and_done();
        let meter = OpenAiMeter::with_limit(true, bytes.len());
        assert_eq!(run(meter, &[&bytes]), (true, Ok(EXPECTED)));
    }

    #[test]
    fn stream_keeps_the_first_error_when_a_later_line_overflows() {
        let mut bytes = b"data: [1]".to_vec();
        bytes.push(b'\n');
        bytes.push(b'\n');
        bytes.extend(vec![b'x'; 20]);
        let mut meter = OpenAiMeter::with_limit(true, 16);
        assert_eq!(meter.feed(&bytes), Err(MeterError::InvalidEvent));
        assert!(meter.ended());
        assert_eq!(meter.finish(), Err(MeterError::InvalidEvent));
    }
}
