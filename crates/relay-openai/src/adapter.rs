use std::collections::BTreeMap;

use billing::model::{ModelUsage, TokenUsage};
use kernel::model::ModelId;
use router::model::Prepared;
use router::ports::Adapter;
use serde_json::{Map, Value};

use crate::error::{ConfigError, PrepareError};
use crate::meter::OpenAiMeter;

const REJECTED_FIELDS: [&str; 3] = ["audio", "prediction", "web_search_options"];
const TEXT_PART_TYPES: [&str; 2] = ["text", "refusal"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLimit {
    pub model: ModelId,
    pub max_output_tokens: u64,
}

/// 只接受配置里列出的模型，模型名按原样精确匹配。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiChat {
    max_output: BTreeMap<String, u64>,
}

impl OpenAiChat {
    /// # Errors
    ///
    /// 列表为空、模型名为空、最大输出为 0，或同一个模型出现两次时返回 `ConfigError`。
    pub fn new(models: impl IntoIterator<Item = ModelLimit>) -> Result<Self, ConfigError> {
        let mut max_output = BTreeMap::new();
        for ModelLimit {
            model,
            max_output_tokens,
        } in models
        {
            if model.0.is_empty() {
                return Err(ConfigError::EmptyModel);
            }
            if max_output_tokens == 0 {
                return Err(ConfigError::ZeroMaxOutput(model));
            }
            if max_output.contains_key(&model.0) {
                return Err(ConfigError::DuplicateModel(model));
            }
            max_output.insert(model.0, max_output_tokens);
        }
        if max_output.is_empty() {
            return Err(ConfigError::NoModels);
        }
        Ok(Self { max_output })
    }
}

impl Adapter for OpenAiChat {
    type Error = PrepareError;
    type Meter = OpenAiMeter;

    fn prepare(&self, body: &[u8]) -> Result<Prepared<OpenAiMeter>, PrepareError> {
        let Ok(Value::Object(mut request)) = serde_json::from_slice::<Value>(body) else {
            return Err(PrepareError::InvalidBody);
        };
        let model = model(&request)?;
        let streaming = streaming(&request)?;
        check_service_tier(&request)?;
        check_n(&request)?;
        check_modalities(&request)?;
        for name in REJECTED_FIELDS {
            if present(&request, name).is_some() {
                return Err(PrepareError::UnsupportedParameter(name));
            }
        }
        check_messages(&request)?;
        let requested = requested_output(&request)?;
        let Some(&model_max) = self.max_output.get(&model) else {
            return Err(PrepareError::ModelNotFound);
        };
        let output_tokens = match requested {
            Some(tokens) if tokens > model_max => {
                return Err(PrepareError::MaxTokensExceedsModelLimit)
            }
            Some(tokens) => tokens,
            None => model_max,
        };
        if streaming {
            include_usage(&mut request)?;
        }
        request.insert("service_tier".into(), Value::from("default"));
        Ok(Prepared {
            limit: ModelUsage {
                model: ModelId(model),
                tokens: TokenUsage {
                    input_tokens: u64::try_from(body.len()).unwrap_or(u64::MAX),
                    cached_input_tokens: 0,
                    output_tokens,
                },
            },
            upstream_body: Value::Object(request).to_string().into_bytes(),
            streaming,
            meter: OpenAiMeter::new(streaming),
        })
    }
}

fn present<'a>(request: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    request.get(name).filter(|value| !value.is_null())
}

fn model(request: &Map<String, Value>) -> Result<String, PrepareError> {
    let Some(Value::String(model)) = present(request, "model") else {
        return Err(PrepareError::InvalidBody);
    };
    if model.is_empty() {
        return Err(PrepareError::InvalidBody);
    }
    Ok(model.clone())
}

fn streaming(request: &Map<String, Value>) -> Result<bool, PrepareError> {
    match present(request, "stream") {
        None => Ok(false),
        Some(Value::Bool(stream)) => Ok(*stream),
        Some(_) => Err(PrepareError::InvalidBody),
    }
}

fn check_service_tier(request: &Map<String, Value>) -> Result<(), PrepareError> {
    match present(request, "service_tier") {
        None => Ok(()),
        Some(Value::String(tier)) if tier == "default" => Ok(()),
        Some(Value::String(_)) => Err(PrepareError::UnsupportedServiceTier),
        Some(_) => Err(PrepareError::InvalidBody),
    }
}

fn check_n(request: &Map<String, Value>) -> Result<(), PrepareError> {
    let Some(n) = present(request, "n") else {
        return Ok(());
    };
    match n.as_u64() {
        Some(1) => Ok(()),
        Some(_) => Err(PrepareError::UnsupportedParameter("n")),
        None => Err(PrepareError::InvalidBody),
    }
}

fn check_modalities(request: &Map<String, Value>) -> Result<(), PrepareError> {
    let Some(modalities) = present(request, "modalities") else {
        return Ok(());
    };
    let Value::Array(modalities) = modalities else {
        return Err(PrepareError::InvalidBody);
    };
    for modality in modalities {
        let Value::String(name) = modality else {
            return Err(PrepareError::InvalidBody);
        };
        if name != "text" {
            return Err(PrepareError::UnsupportedParameter("modalities"));
        }
    }
    Ok(())
}

fn check_messages(request: &Map<String, Value>) -> Result<(), PrepareError> {
    let Some(Value::Array(messages)) = present(request, "messages") else {
        return Err(PrepareError::InvalidBody);
    };
    for message in messages {
        let Value::Object(message) = message else {
            return Err(PrepareError::InvalidBody);
        };
        match present(message, "content") {
            None | Some(Value::String(_)) => {}
            Some(Value::Array(parts)) => check_parts(parts)?,
            Some(_) => return Err(PrepareError::InvalidBody),
        }
    }
    Ok(())
}

fn check_parts(parts: &[Value]) -> Result<(), PrepareError> {
    for part in parts {
        let Value::Object(part) = part else {
            return Err(PrepareError::InvalidBody);
        };
        let Some(Value::String(kind)) = part.get("type") else {
            return Err(PrepareError::InvalidBody);
        };
        if !TEXT_PART_TYPES.contains(&kind.as_str()) {
            return Err(PrepareError::UnsupportedParameter("content"));
        }
    }
    Ok(())
}

fn requested_output(request: &Map<String, Value>) -> Result<Option<u64>, PrepareError> {
    let completion = positive(request, "max_completion_tokens")?;
    let legacy = positive(request, "max_tokens")?;
    match (completion, legacy) {
        (Some(_), Some(_)) => Err(PrepareError::ConflictingMaxTokens),
        (Some(tokens), None) | (None, Some(tokens)) => Ok(Some(tokens)),
        (None, None) => Ok(None),
    }
}

fn positive(request: &Map<String, Value>, name: &str) -> Result<Option<u64>, PrepareError> {
    match present(request, name) {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|tokens| *tokens > 0)
            .map(Some)
            .ok_or(PrepareError::InvalidBody),
    }
}

fn include_usage(request: &mut Map<String, Value>) -> Result<(), PrepareError> {
    let options = request
        .entry("stream_options")
        .or_insert_with(|| Value::Object(Map::new()));
    if options.is_null() {
        *options = Value::Object(Map::new());
    }
    let Value::Object(options) = options else {
        return Err(PrepareError::InvalidBody);
    };
    options.insert("include_usage".into(), Value::Bool(true));
    Ok(())
}
