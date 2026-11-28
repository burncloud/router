use billing::model::TokenUsage;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::MeterError;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    prompt_tokens_details: PromptDetails,
    completion_tokens_details: CompletionDetails,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptDetails {
    cached_tokens: u64,
    audio_tokens: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletionDetails {
    #[serde(rename = "reasoning_tokens")]
    reasoning: u64,
    #[serde(rename = "audio_tokens")]
    audio: u64,
    #[serde(rename = "accepted_prediction_tokens")]
    accepted_prediction: u64,
    #[serde(rename = "rejected_prediction_tokens")]
    rejected_prediction: u64,
}

pub(crate) fn token_usage(value: &Value) -> Result<TokenUsage, MeterError> {
    let wire = WireUsage::deserialize(value).map_err(|_| MeterError::InvalidUsage)?;
    if wire.prompt_tokens_details.audio_tokens != 0 || wire.completion_tokens_details.audio != 0 {
        return Err(MeterError::UnsupportedBillingItem("audio_tokens"));
    }
    if wire.completion_tokens_details.accepted_prediction != 0
        || wire.completion_tokens_details.rejected_prediction != 0
    {
        return Err(MeterError::UnsupportedBillingItem("prediction_tokens"));
    }
    if wire.prompt_tokens.checked_add(wire.completion_tokens) != Some(wire.total_tokens) {
        return Err(MeterError::InconsistentTotal);
    }
    if wire.completion_tokens_details.reasoning > wire.completion_tokens {
        return Err(MeterError::ReasoningExceedsCompletion);
    }
    let cached = wire.prompt_tokens_details.cached_tokens;
    let Some(input_tokens) = wire.prompt_tokens.checked_sub(cached) else {
        return Err(MeterError::CachedExceedsPrompt);
    };
    Ok(TokenUsage {
        input_tokens,
        cached_input_tokens: cached,
        output_tokens: wire.completion_tokens,
    })
}

pub(crate) fn check_service_tier(object: &Map<String, Value>) -> Result<(), MeterError> {
    match object.get("service_tier") {
        None => Ok(()),
        Some(Value::String(tier)) if tier == "default" => Ok(()),
        Some(_) => Err(MeterError::UnsupportedServiceTier),
    }
}

pub(crate) fn check_error(object: &Map<String, Value>) -> Result<(), MeterError> {
    match object.get("error") {
        None | Some(Value::Null) => Ok(()),
        Some(_) => Err(MeterError::UpstreamError),
    }
}
