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
        }
    }
}

impl StreamState {
    fn feed(&mut self, bytes: &[u8], failure: &mut Option<MeterError>) {
        for event in self.splitter.push(bytes) {
            let outcome = self.event(&event, failure.is_some());
            if let (Err(error), None) = (outcome, *failure) {
                *failure = Some(error);
            }
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

impl UsageMeter for OpenAiMeter {
    type Error = MeterError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), MeterError> {
        match &mut self.mode {
            Mode::Stream(state) => state.feed(bytes, &mut self.failure),
            Mode::Whole(body) => body.extend_from_slice(bytes),
        }
        self.failure.map_or(Ok(()), Err)
    }

    fn ended(&self) -> bool {
        match &self.mode {
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

const _: () = {
    const fn assert_auto_traits<
        T: Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
    >() {
    }
    assert_auto_traits::<OpenAiMeter>();
};
