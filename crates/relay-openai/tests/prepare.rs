use billing::model::TokenUsage;
use kernel::model::ModelId;
use relay_openai::{ConfigError, ModelLimit, OpenAiChat, PrepareError};
use router::ports::Adapter;
use serde_json::{json, Value};

fn limit(model: &str, max_output_tokens: u64) -> ModelLimit {
    ModelLimit {
        model: ModelId(model.into()),
        max_output_tokens,
    }
}

fn chat() -> OpenAiChat {
    OpenAiChat::new([limit("gpt-4o-mini", 16_384), limit("gpt-long", 100)]).unwrap()
}

fn request(extra: &Value) -> Vec<u8> {
    let mut body = json!({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "Say hello."}],
    });
    for (key, value) in extra.as_object().unwrap() {
        body.as_object_mut()
            .unwrap()
            .insert(key.clone(), value.clone());
    }
    body.to_string().into_bytes()
}

fn rejected(extra: &Value) -> PrepareError {
    chat().prepare(&request(extra)).unwrap_err()
}

fn upstream(extra: &Value) -> Value {
    serde_json::from_slice(&chat().prepare(&request(extra)).unwrap().upstream_body).unwrap()
}

#[test]
fn config_rejects_bad_model_lists() {
    assert_eq!(OpenAiChat::new([]), Err(ConfigError::NoModels));
    assert_eq!(
        OpenAiChat::new([limit("", 1)]),
        Err(ConfigError::EmptyModel)
    );
    assert_eq!(
        OpenAiChat::new([limit("m", 0)]),
        Err(ConfigError::ZeroMaxOutput(ModelId("m".into())))
    );
    assert_eq!(
        OpenAiChat::new([limit("m", 1), limit("m", 2)]),
        Err(ConfigError::DuplicateModel(ModelId("m".into())))
    );
}

#[test]
fn limit_uses_request_bytes_and_model_maximum() {
    let body = request(&json!({}));
    let prepared = chat().prepare(&body).unwrap();
    assert_eq!(prepared.limit.model, ModelId("gpt-4o-mini".into()));
    assert_eq!(
        prepared.limit.tokens,
        TokenUsage {
            input_tokens: u64::try_from(body.len()).unwrap(),
            cached_input_tokens: 0,
            output_tokens: 16_384,
        }
    );
    assert!(!prepared.streaming);
}

#[test]
fn max_completion_tokens_or_max_tokens_set_the_output_limit() {
    let by_completion = chat()
        .prepare(&request(&json!({"max_completion_tokens": 30})))
        .unwrap();
    assert_eq!(by_completion.limit.tokens.output_tokens, 30);
    let by_legacy = chat()
        .prepare(&request(&json!({"max_tokens": 20})))
        .unwrap();
    assert_eq!(by_legacy.limit.tokens.output_tokens, 20);
    let at_limit = chat()
        .prepare(&request(&json!({"model": "gpt-long", "max_tokens": 100})))
        .unwrap();
    assert_eq!(at_limit.limit.tokens.output_tokens, 100);
    let null_is_absent = chat()
        .prepare(&request(
            &json!({"max_tokens": null, "max_completion_tokens": 7}),
        ))
        .unwrap();
    assert_eq!(null_is_absent.limit.tokens.output_tokens, 7);
}

#[test]
fn output_limit_violations_are_rejected() {
    assert_eq!(
        rejected(&json!({"model": "gpt-long", "max_tokens": 101})),
        PrepareError::MaxTokensExceedsModelLimit
    );
    assert_eq!(
        rejected(&json!({"max_tokens": 10, "max_completion_tokens": 10})),
        PrepareError::ConflictingMaxTokens
    );
    for bad in [json!(0), json!(-1), json!(1.5), json!("10")] {
        assert_eq!(
            rejected(&json!({"max_tokens": bad.clone()})),
            PrepareError::InvalidBody
        );
        assert_eq!(
            rejected(&json!({"max_completion_tokens": bad})),
            PrepareError::InvalidBody
        );
    }
}

#[test]
fn models_match_exactly() {
    for model in ["GPT-4o-mini", "gpt-4o-mini-2024-07-18", "gpt-4o", "unknown"] {
        assert_eq!(
            rejected(&json!({"model": model})),
            PrepareError::ModelNotFound,
            "{model}"
        );
    }
}

#[test]
fn service_tier_only_allows_default() {
    for tier in ["auto", "flex", "priority", "scale", "Default"] {
        assert_eq!(
            rejected(&json!({"service_tier": tier})),
            PrepareError::UnsupportedServiceTier,
            "{tier}"
        );
    }
    assert_eq!(
        rejected(&json!({"service_tier": 1})),
        PrepareError::InvalidBody
    );
    for allowed in [
        json!({}),
        json!({"service_tier": null}),
        json!({"service_tier": "default"}),
    ] {
        assert_eq!(upstream(&allowed)["service_tier"], json!("default"));
    }
}

#[test]
fn unsupported_parameters_are_rejected() {
    let cases = [
        (json!({"n": 2}), "n"),
        (json!({"n": 0}), "n"),
        (json!({"modalities": ["text", "audio"]}), "modalities"),
        (
            json!({"audio": {"voice": "alloy", "format": "wav"}}),
            "audio",
        ),
        (
            json!({"prediction": {"type": "content", "content": "x"}}),
            "prediction",
        ),
        (json!({"web_search_options": {}}), "web_search_options"),
        (
            json!({"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "https://example.com/a.png"}}]}]}),
            "content",
        ),
        (
            json!({"messages": [{"role": "user", "content": [{"type": "input_audio", "input_audio": {}}]}]}),
            "content",
        ),
        (
            json!({"messages": [{"role": "user", "content": [{"type": "file", "file": {}}]}]}),
            "content",
        ),
    ];
    for (extra, name) in cases {
        assert_eq!(
            rejected(&extra),
            PrepareError::UnsupportedParameter(name),
            "{extra}"
        );
    }
}

#[test]
fn text_only_requests_are_accepted() {
    let accepted = [
        json!({"n": 1}),
        json!({"n": null, "audio": null, "prediction": null, "web_search_options": null}),
        json!({"modalities": ["text"]}),
        json!({"messages": [
            {"role": "system", "content": "Be brief."},
            {"role": "user", "content": [{"type": "text", "text": "hi"}]},
            {"role": "assistant", "content": [{"type": "refusal", "refusal": "no"}]},
            {"role": "assistant", "content": null, "tool_calls": []},
            {"role": "tool", "tool_call_id": "t", "content": "ok"},
        ]}),
    ];
    for extra in accepted {
        assert!(chat().prepare(&request(&extra)).is_ok(), "{extra}");
    }
}

#[test]
fn malformed_bodies_are_invalid() {
    let raw: [&[u8]; 4] = [b"", b"not json", b"[]", b"{\"model\": \"gpt-4o-mini\""];
    for body in raw {
        assert_eq!(chat().prepare(body).unwrap_err(), PrepareError::InvalidBody);
    }
    let cases = [
        json!({"model": ""}),
        json!({"model": 1}),
        json!({"model": null}),
        json!({"stream": "yes"}),
        json!({"n": "1"}),
        json!({"n": 1.5}),
        json!({"modalities": "text"}),
        json!({"modalities": [1]}),
        json!({"messages": null}),
        json!({"messages": {}}),
        json!({"messages": ["hi"]}),
        json!({"messages": [{"role": "user", "content": 1}]}),
        json!({"messages": [{"role": "user", "content": ["hi"]}]}),
        json!({"messages": [{"role": "user", "content": [{"text": "hi"}]}]}),
        json!({"messages": [{"role": "user", "content": [{"type": 1}]}]}),
        json!({"stream": true, "stream_options": "on"}),
    ];
    for extra in cases {
        assert_eq!(rejected(&extra), PrepareError::InvalidBody, "{extra}");
    }
    let missing = json!({"model": "gpt-4o-mini"}).to_string().into_bytes();
    assert_eq!(
        chat().prepare(&missing).unwrap_err(),
        PrepareError::InvalidBody
    );
}

#[test]
fn streaming_always_includes_usage() {
    for extra in [
        json!({"stream": true}),
        json!({"stream": true, "stream_options": null}),
        json!({"stream": true, "stream_options": {"include_usage": false}}),
        json!({"stream": true, "stream_options": {"include_usage": true}}),
    ] {
        let prepared = chat().prepare(&request(&extra)).unwrap();
        assert!(prepared.streaming);
        let body: Value = serde_json::from_slice(&prepared.upstream_body).unwrap();
        assert_eq!(
            body["stream_options"]["include_usage"],
            json!(true),
            "{extra}"
        );
    }
}

#[test]
fn only_stream_options_and_service_tier_change() {
    let extra = json!({
        "stream": true,
        "stream_options": {"include_usage": false, "include_obfuscation": false},
        "temperature": 0.25,
        "tools": [{"type": "function", "function": {"name": "f", "parameters": {"type": "object"}}}],
        "user": "u-1",
        "max_tokens": 20,
    });
    let mut expected: Value = serde_json::from_slice(&request(&extra)).unwrap();
    expected["stream_options"]["include_usage"] = json!(true);
    expected["service_tier"] = json!("default");
    assert_eq!(upstream(&extra), expected);
}

#[test]
fn non_streaming_body_is_untouched_except_service_tier() {
    let extra = json!({"stream": false, "stream_options": {"include_usage": false}});
    let mut expected: Value = serde_json::from_slice(&request(&extra)).unwrap();
    expected["service_tier"] = json!("default");
    assert_eq!(upstream(&extra), expected);
}

#[test]
fn error_codes_match_the_error_table() {
    let codes = [
        (PrepareError::InvalidBody, "invalid_request_body"),
        (PrepareError::ModelNotFound, "model_not_found"),
        (
            PrepareError::UnsupportedServiceTier,
            "unsupported_service_tier",
        ),
        (
            PrepareError::UnsupportedParameter("n"),
            "unsupported_parameter",
        ),
        (
            PrepareError::MaxTokensExceedsModelLimit,
            "max_tokens_exceeds_model_limit",
        ),
        (PrepareError::ConflictingMaxTokens, "conflicting_max_tokens"),
    ];
    for (error, code) in codes {
        assert_eq!(error.code(), code);
    }
}

#[test]
fn error_messages_do_not_echo_the_request() {
    let error = rejected(&json!({"model": "secret-model-name"}));
    assert!(!error.to_string().contains("secret"));
}

#[test]
fn upstream_body_keeps_key_order_and_large_integers() {
    let tools = r#"{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"zebra":{"type":"string"},"alpha":{"type":"integer"}},"required":["zebra","alpha"]}}}"#;
    let format = r#"{"type":"json_schema","json_schema":{"name":"out","strict":true,"schema":{"type":"object","properties":{"last":{"type":"string"},"first":{"type":"string"}},"required":["last","first"]}}}"#;
    let messages = r#"[{"role":"user","content":"Say hello."}]"#;

    let no_stream = format!(
        r#"{{"model":"gpt-4o-mini","messages":{messages},"tools":[{tools}],"response_format":{format},"temperature":0.25,"seed":18446744073709551616,"also":9007199254740993,"user":"u-1"}}"#
    );
    let expected_no_stream = format!(
        r#"{{"model":"gpt-4o-mini","messages":{messages},"tools":[{tools}],"response_format":{format},"temperature":0.25,"seed":18446744073709551616,"also":9007199254740993,"user":"u-1","service_tier":"default"}}"#
    );
    assert_eq!(
        chat().prepare(no_stream.as_bytes()).unwrap().upstream_body,
        expected_no_stream.as_bytes()
    );

    let with_options = format!(
        r#"{{"model":"gpt-4o-mini","messages":{messages},"stream":true,"stream_options":{{"include_usage":false,"include_obfuscation":false}},"tools":[{tools}],"response_format":{format},"max_tokens":20,"seed":-9223372036854775809}}"#
    );
    let expected_with_options = format!(
        r#"{{"model":"gpt-4o-mini","messages":{messages},"stream":true,"stream_options":{{"include_usage":true,"include_obfuscation":false}},"tools":[{tools}],"response_format":{format},"max_tokens":20,"seed":-9223372036854775809,"service_tier":"default"}}"#
    );
    assert_eq!(
        chat()
            .prepare(with_options.as_bytes())
            .unwrap()
            .upstream_body,
        expected_with_options.as_bytes()
    );

    let tier_already = format!(
        r#"{{"model":"gpt-4o-mini","service_tier":"default","messages":{messages},"stream":true,"seed":1e2}}"#
    );
    let expected_tier_already = format!(
        r#"{{"model":"gpt-4o-mini","service_tier":"default","messages":{messages},"stream":true,"seed":1e+2,"stream_options":{{"include_usage":true}}}}"#
    );
    assert_eq!(
        chat()
            .prepare(tier_already.as_bytes())
            .unwrap()
            .upstream_body,
        expected_tier_already.as_bytes()
    );
}

#[test]
fn pretty_printed_request_is_rewritten_compact_with_the_same_key_order() {
    let body = "{\n  \"model\": \"gpt-4o-mini\",\n  \"messages\": [{\"role\": \"user\", \"content\": \"Say hello.\"}],\n  \"seed\": 18446744073709551616\n}";
    let expected = r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Say hello."}],"seed":18446744073709551616,"service_tier":"default"}"#;
    assert_eq!(
        chat().prepare(body.as_bytes()).unwrap().upstream_body,
        expected.as_bytes()
    );
}
