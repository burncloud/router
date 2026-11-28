use billing::model::TokenUsage;
use kernel::model::ModelId;
use proptest::prelude::*;
use relay_openai::{MeterError, ModelLimit, OpenAiChat, OpenAiMeter};
use router::ports::{Adapter, UsageMeter};
use serde_json::{json, Value};

const CONTENT_CHUNK: &str = r#"{"id":"c1","object":"chat.completion.chunk","model":"gpt-4o-mini-2024-07-18","service_tier":"default","choices":[{"index":0,"delta":{"content":"Hi 你好"},"finish_reason":null}],"usage":null}"#;
const STOP_CHUNK: &str = r#"{"id":"c1","object":"chat.completion.chunk","model":"gpt-4o-mini-2024-07-18","service_tier":"default","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":null}"#;
const USAGE: &str = r#"{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20,"prompt_tokens_details":{"cached_tokens":3,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":2,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}}"#;

const EXPECTED: TokenUsage = TokenUsage {
    input_tokens: 10,
    cached_input_tokens: 3,
    output_tokens: 7,
};

fn usage_chunk(usage: &str) -> String {
    format!(
        r#"{{"id":"c1","object":"chat.completion.chunk","model":"gpt-4o-mini-2024-07-18","choices":[],"usage":{usage}}}"#
    )
}

fn sse(events: &[&str], separator: &str) -> Vec<u8> {
    let mut out = String::new();
    for event in events {
        out.push_str("data: ");
        out.push_str(event);
        out.push_str(separator);
    }
    out.into_bytes()
}

fn full_stream(separator: &str) -> Vec<u8> {
    let usage = usage_chunk(USAGE);
    sse(
        &[CONTENT_CHUNK, CONTENT_CHUNK, STOP_CHUNK, &usage, "[DONE]"],
        separator,
    )
}

fn meter(streaming: bool) -> OpenAiMeter {
    let chat = OpenAiChat::new([ModelLimit {
        model: ModelId("gpt-4o-mini".into()),
        max_output_tokens: 100,
    }])
    .unwrap();
    let body = json!({
        "model": "gpt-4o-mini",
        "stream": streaming,
        "messages": [{"role": "user", "content": "hi"}],
    });
    chat.prepare(body.to_string().as_bytes()).unwrap().meter
}

fn run(streaming: bool, pieces: &[&[u8]]) -> (bool, Result<TokenUsage, MeterError>) {
    let mut meter = meter(streaming);
    for piece in pieces {
        let _ = meter.feed(piece);
    }
    let ended = meter.ended();
    (ended, meter.finish())
}

fn stream(bytes: &[u8]) -> (bool, Result<TokenUsage, MeterError>) {
    run(true, &[bytes])
}

fn whole(body: &Value) -> (bool, Result<TokenUsage, MeterError>) {
    run(false, &[body.to_string().as_bytes()])
}

fn completion(usage: &str) -> Value {
    json!({
        "id": "c1",
        "object": "chat.completion",
        "model": "gpt-4o-mini-2024-07-18",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}],
        "usage": serde_json::from_str::<Value>(usage).unwrap(),
        "service_tier": "default",
    })
}

fn usage_with(path: &[&str], value: &Value) -> String {
    let mut usage: Value = serde_json::from_str(USAGE).unwrap();
    let mut target = &mut usage;
    for key in &path[..path.len() - 1] {
        target = &mut target[*key];
    }
    target[path[path.len() - 1]] = value.clone();
    usage.to_string()
}

fn usage_without(path: &[&str]) -> String {
    let mut usage: Value = serde_json::from_str(USAGE).unwrap();
    let mut target = &mut usage;
    for key in &path[..path.len() - 1] {
        target = &mut target[*key];
    }
    target.as_object_mut().unwrap().remove(path[path.len() - 1]);
    usage.to_string()
}

#[test]
fn stream_reads_usage_with_lf_and_crlf() {
    assert_eq!(stream(&full_stream("\n\n")), (true, Ok(EXPECTED)));
    assert_eq!(stream(&full_stream("\r\n\r\n")), (true, Ok(EXPECTED)));
}

#[test]
fn stream_reads_usage_byte_by_byte() {
    let bytes = full_stream("\n\n");
    let pieces: Vec<&[u8]> = bytes.chunks(1).collect();
    assert_eq!(run(true, &pieces), (true, Ok(EXPECTED)));
}

#[test]
fn stream_feed_succeeds_on_good_chunks() {
    let mut meter = meter(true);
    assert_eq!(meter.feed(&full_stream("\n\n")), Ok(()));
    assert!(meter.ended());
}

#[test]
fn stream_without_usage_ends_but_has_no_usage() {
    let bytes = sse(&[CONTENT_CHUNK, STOP_CHUNK, "[DONE]"], "\n\n");
    assert_eq!(stream(&bytes), (true, Err(MeterError::MissingUsage)));
}

#[test]
fn stream_cut_before_done_is_not_ended() {
    let bytes = sse(&[CONTENT_CHUNK], "\n\n");
    assert_eq!(stream(&bytes), (false, Err(MeterError::MissingUsage)));
    let usage = usage_chunk(USAGE);
    let with_usage = sse(&[CONTENT_CHUNK, &usage], "\n\n");
    assert_eq!(stream(&with_usage), (false, Ok(EXPECTED)));
}

#[test]
fn done_without_blank_line_is_not_ended() {
    let mut bytes = sse(&[CONTENT_CHUNK], "\n\n");
    bytes.extend_from_slice(b"data: [DONE]\n");
    assert_eq!(stream(&bytes), (false, Err(MeterError::MissingUsage)));
}

#[test]
fn second_usage_is_an_error() {
    let usage = usage_chunk(USAGE);
    let bytes = sse(&[&usage, &usage, "[DONE]"], "\n\n");
    assert_eq!(stream(&bytes), (true, Err(MeterError::DuplicateUsage)));
}

#[test]
fn data_after_done_is_an_error() {
    let usage = usage_chunk(USAGE);
    let bytes = sse(&[&usage, "[DONE]", CONTENT_CHUNK], "\n\n");
    assert_eq!(stream(&bytes), (true, Err(MeterError::DataAfterDone)));
}

#[test]
fn bad_event_errors_but_done_is_still_tracked() {
    let usage = usage_chunk(USAGE);
    let bytes = sse(&["{not json", &usage, "[DONE]"], "\n\n");
    let mut meter = meter(true);
    assert_eq!(meter.feed(&bytes), Err(MeterError::InvalidEvent));
    assert_eq!(meter.feed(b""), Err(MeterError::InvalidEvent));
    assert!(meter.ended());
    assert_eq!(meter.finish(), Err(MeterError::InvalidEvent));
}

#[test]
fn first_error_is_kept() {
    let bytes = sse(&["[1]", r#"{"error":{"message":"x"}}"#, "[DONE]"], "\n\n");
    assert_eq!(stream(&bytes), (true, Err(MeterError::InvalidEvent)));
}

#[test]
fn upstream_error_event_is_an_error() {
    let bytes = sse(
        &[CONTENT_CHUNK, r#"{"error":{"message":"overloaded"}}"#],
        "\n\n",
    );
    assert_eq!(stream(&bytes), (false, Err(MeterError::UpstreamError)));
}

#[test]
fn non_default_service_tier_in_response_is_an_error() {
    for tier in [json!("priority"), json!("flex"), json!(null)] {
        let chunk = format!(r#"{{"service_tier":{tier},"choices":[],"usage":null}}"#);
        let usage = usage_chunk(USAGE);
        let bytes = sse(&[&chunk, &usage, "[DONE]"], "\n\n");
        assert_eq!(
            stream(&bytes),
            (true, Err(MeterError::UnsupportedServiceTier)),
            "{tier}"
        );
        let mut body = completion(USAGE);
        body["service_tier"] = tier.clone();
        assert_eq!(
            whole(&body),
            (true, Err(MeterError::UnsupportedServiceTier))
        );
    }
}

#[test]
fn whole_body_reads_top_level_usage() {
    assert_eq!(whole(&completion(USAGE)), (true, Ok(EXPECTED)));
    let mut no_tier = completion(USAGE);
    no_tier.as_object_mut().unwrap().remove("service_tier");
    assert_eq!(whole(&no_tier), (true, Ok(EXPECTED)));
}

#[test]
fn whole_body_split_anywhere_reads_the_same() {
    let body = serde_json::to_vec_pretty(&completion(USAGE)).unwrap();
    let pieces: Vec<&[u8]> = body.chunks(3).collect();
    assert_eq!(run(false, &pieces), (true, Ok(EXPECTED)));
}

#[test]
fn whole_body_without_usage_ends_but_has_no_usage() {
    let mut missing = completion(USAGE);
    missing.as_object_mut().unwrap().remove("usage");
    assert_eq!(whole(&missing), (true, Err(MeterError::MissingUsage)));
    let mut null = completion(USAGE);
    null["usage"] = Value::Null;
    assert_eq!(whole(&null), (true, Err(MeterError::MissingUsage)));
}

#[test]
fn incomplete_whole_body_is_not_ended() {
    let body = completion(USAGE).to_string();
    let cut = &body.as_bytes()[..body.len() - 1];
    assert_eq!(run(false, &[cut]), (false, Err(MeterError::IncompleteBody)));
}

#[test]
fn whole_body_end_follows_later_bytes() {
    let body = completion(USAGE).to_string();
    let (head, tail) = body.as_bytes().split_at(body.len() - 1);
    let mut meter = meter(false);
    assert_eq!(meter.feed(head), Ok(()));
    assert!(!meter.ended());
    assert!(!meter.ended());
    assert_eq!(meter.feed(tail), Ok(()));
    assert!(meter.ended());
    assert!(meter.ended());
    let snapshot = meter.clone();
    assert_eq!(meter.feed(b"x"), Ok(()));
    assert!(!meter.ended());
    assert_ne!(meter, snapshot);
    assert!(snapshot.ended());
}

#[test]
fn whole_body_feed_only_collects_bytes() {
    let body = completion(USAGE).to_string();
    let mut partial = meter(false);
    for byte in b"{\"usage\": nonsense" {
        assert_eq!(partial.feed(&[*byte]), Ok(()));
    }
    assert!(!partial.ended());
    assert_eq!(partial.finish(), Err(MeterError::IncompleteBody));
    let mut complete = meter(false);
    for byte in body.as_bytes() {
        assert_eq!(complete.feed(&[*byte]), Ok(()));
    }
    assert!(complete.ended());
    assert_eq!(complete.finish(), Ok(EXPECTED));
}

#[test]
fn whole_body_must_be_an_object_without_error() {
    assert_eq!(run(false, &[b"[]"]), (true, Err(MeterError::InvalidEvent)));
    let error = json!({"error": {"message": "x"}});
    assert_eq!(whole(&error), (true, Err(MeterError::UpstreamError)));
}

#[test]
fn bad_usage_is_never_zero() {
    let cases = [
        (
            usage_with(&["prompt_tokens"], &json!(-1)),
            MeterError::InvalidUsage,
        ),
        (
            usage_with(&["prompt_tokens"], &json!(1.5)),
            MeterError::InvalidUsage,
        ),
        (
            usage_with(&["prompt_tokens"], &json!("13")),
            MeterError::InvalidUsage,
        ),
        (
            usage_with(
                &["completion_tokens"],
                &json!(18_446_744_073_709_551_616_f64),
            ),
            MeterError::InvalidUsage,
        ),
        (usage_without(&["prompt_tokens"]), MeterError::InvalidUsage),
        (usage_without(&["total_tokens"]), MeterError::InvalidUsage),
        (
            usage_without(&["prompt_tokens_details"]),
            MeterError::InvalidUsage,
        ),
        (
            usage_without(&["prompt_tokens_details", "cached_tokens"]),
            MeterError::InvalidUsage,
        ),
        (
            usage_without(&["completion_tokens_details"]),
            MeterError::InvalidUsage,
        ),
        (
            usage_with(&["new_billed_item"], &json!(0)),
            MeterError::InvalidUsage,
        ),
        (
            usage_with(&["prompt_tokens_details", "image_tokens"], &json!(0)),
            MeterError::InvalidUsage,
        ),
        (
            usage_with(&["prompt_tokens_details", "cached_tokens"], &json!(14)),
            MeterError::CachedExceedsPrompt,
        ),
        (
            usage_with(&["prompt_tokens_details", "audio_tokens"], &json!(1)),
            MeterError::UnsupportedBillingItem("audio_tokens"),
        ),
        (
            usage_with(&["completion_tokens_details", "audio_tokens"], &json!(1)),
            MeterError::UnsupportedBillingItem("audio_tokens"),
        ),
        (
            usage_with(
                &["completion_tokens_details", "accepted_prediction_tokens"],
                &json!(1),
            ),
            MeterError::UnsupportedBillingItem("prediction_tokens"),
        ),
        (
            usage_with(
                &["completion_tokens_details", "rejected_prediction_tokens"],
                &json!(1),
            ),
            MeterError::UnsupportedBillingItem("prediction_tokens"),
        ),
        (
            usage_with(&["total_tokens"], &json!(21)),
            MeterError::InconsistentTotal,
        ),
        (
            usage_with(
                &["completion_tokens_details", "reasoning_tokens"],
                &json!(8),
            ),
            MeterError::ReasoningExceedsCompletion,
        ),
        ("null".to_owned(), MeterError::MissingUsage),
        ("[]".to_owned(), MeterError::InvalidUsage),
    ];
    for (usage, error) in cases {
        let usage_event = usage_chunk(&usage);
        let bytes = sse(&[&usage_event, "[DONE]"], "\n\n");
        assert_eq!(stream(&bytes), (true, Err(error)), "stream {usage}");
        assert_eq!(
            whole(&completion(&usage)),
            (true, Err(error)),
            "whole {usage}"
        );
    }
}

#[test]
fn total_overflow_is_inconsistent() {
    let usage = json!({
        "prompt_tokens": u64::MAX,
        "completion_tokens": 1,
        "total_tokens": u64::MAX,
        "prompt_tokens_details": {"cached_tokens": 0, "audio_tokens": 0},
        "completion_tokens_details": {"reasoning_tokens": 0, "audio_tokens": 0, "accepted_prediction_tokens": 0, "rejected_prediction_tokens": 0},
    })
    .to_string();
    assert_eq!(
        whole(&completion(&usage)),
        (true, Err(MeterError::InconsistentTotal))
    );
}

#[test]
fn cached_equal_to_prompt_leaves_no_uncached_input() {
    let usage = usage_with(&["prompt_tokens_details", "cached_tokens"], &json!(13));
    assert_eq!(
        whole(&completion(&usage)),
        (
            true,
            Ok(TokenUsage {
                input_tokens: 0,
                cached_input_tokens: 13,
                output_tokens: 7,
            })
        )
    );
}

fn split_at(bytes: &[u8], mut cuts: Vec<usize>) -> Vec<&[u8]> {
    cuts.retain(|cut| *cut <= bytes.len());
    cuts.sort_unstable();
    cuts.dedup();
    let mut pieces = Vec::new();
    let mut start = 0;
    for cut in cuts {
        pieces.push(&bytes[start..cut]);
        start = cut;
    }
    pieces.push(&bytes[start..]);
    pieces
}

proptest! {
    #[test]
    fn stream_split_anywhere_reads_the_same(
        crlf in any::<bool>(),
        cuts in proptest::collection::vec(0usize..2_000, 0..40),
    ) {
        let bytes = full_stream(if crlf { "\r\n\r\n" } else { "\n\n" });
        let pieces = split_at(&bytes, cuts);
        prop_assert_eq!(run(true, &pieces), (true, Ok(EXPECTED)));
    }

    #[test]
    fn whole_split_anywhere_reads_the_same(
        cuts in proptest::collection::vec(0usize..1_000, 0..40),
    ) {
        let body = serde_json::to_vec_pretty(&completion(USAGE)).unwrap();
        let pieces = split_at(&body, cuts);
        prop_assert_eq!(run(false, &pieces), (true, Ok(EXPECTED)));
    }

    #[test]
    fn any_valid_usage_converts_exactly(
        prompt in 0u64..=u64::MAX / 2,
        completion_tokens in 0u64..=u64::MAX / 2,
        cached_share in 0u64..=100,
        reasoning_share in 0u64..=100,
    ) {
        let cached = u64::try_from(u128::from(prompt) * u128::from(cached_share) / 100).unwrap();
        let reasoning = u64::try_from(u128::from(completion_tokens) * u128::from(reasoning_share) / 100).unwrap();
        let usage = json!({
            "prompt_tokens": prompt,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt + completion_tokens,
            "prompt_tokens_details": {"cached_tokens": cached, "audio_tokens": 0},
            "completion_tokens_details": {"reasoning_tokens": reasoning, "audio_tokens": 0, "accepted_prediction_tokens": 0, "rejected_prediction_tokens": 0},
        }).to_string();
        let expected = TokenUsage {
            input_tokens: prompt - cached,
            cached_input_tokens: cached,
            output_tokens: completion_tokens,
        };
        prop_assert_eq!(whole(&completion(&usage)), (true, Ok(expected)));
        let event = usage_chunk(&usage);
        prop_assert_eq!(stream(&sse(&[&event, "[DONE]"], "\n\n")), (true, Ok(expected)));
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let (_, stream_result) = run(true, &[&bytes]);
        let (_, whole_result) = run(false, &[&bytes]);
        prop_assert!(stream_result.is_err());
        prop_assert!(whole_result.is_err());
    }
}

#[test]
fn usage_integers_beyond_u64_are_invalid() {
    let too_big = "18446744073709551616";
    let usage = usage_with(&["prompt_tokens"], &serde_json::from_str(too_big).unwrap());
    assert!(usage.contains(too_big), "{usage}");
    let event = usage_chunk(&usage);
    let bytes = sse(&[&event, "[DONE]"], "\n\n");
    assert_eq!(stream(&bytes), (true, Err(MeterError::InvalidUsage)));
    assert_eq!(
        whole(&completion(&usage)),
        (true, Err(MeterError::InvalidUsage))
    );
}
