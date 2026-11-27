use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use tokio::net::TcpListener;

#[derive(Default)]
pub(crate) struct Record {
    hits: Mutex<Vec<Value>>,
    completed: Mutex<Vec<String>>,
}

impl Record {
    pub(crate) fn clear(&self) {
        lock(&self.hits).clear();
        lock(&self.completed).clear();
    }

    pub(crate) fn hits(&self) -> Vec<Value> {
        lock(&self.hits).clone()
    }

    pub(crate) fn completed(&self) -> Vec<String> {
        lock(&self.completed).clone()
    }

    fn complete(&self, path: String) {
        lock(&self.completed).push(path);
    }
}

pub(crate) type Hits = Arc<Record>;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

const COMPLETION_ID: &str = "chatcmpl-baseline-fixed";
const CREATED: i64 = 1_700_000_000;
const SSE_GAP: Duration = Duration::from_millis(20);

fn openai_usage() -> Value {
    json!({"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30})
}

fn openai_ok(model: &str, with_usage: bool) -> Value {
    let mut v = json!({
        "id": COMPLETION_ID,
        "object": "chat.completion",
        "created": CREATED,
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "baseline answer"}, "finish_reason": "stop"}]
    });
    if with_usage {
        v["usage"] = openai_usage();
    }
    v
}

fn openai_chunk(model: &str, choices: &Value) -> Value {
    json!({
        "id": COMPLETION_ID,
        "object": "chat.completion.chunk",
        "created": CREATED,
        "model": model,
        "choices": choices
    })
}

fn openai_sse(model: &str, with_usage: bool) -> Vec<String> {
    let mut events = Vec::new();
    for (i, piece) in ["base", "line ", "answer"].iter().enumerate() {
        let delta = if i == 0 {
            json!({"role": "assistant", "content": piece})
        } else {
            json!({"content": piece})
        };
        let choices = json!([{"index": 0, "delta": delta, "finish_reason": null}]);
        events.push(format!("data: {}\n\n", openai_chunk(model, &choices)));
    }
    let fin = json!([{"index": 0, "delta": {}, "finish_reason": "stop"}]);
    events.push(format!("data: {}\n\n", openai_chunk(model, &fin)));
    if with_usage {
        let mut usage = openai_chunk(model, &json!([]));
        usage["usage"] = openai_usage();
        events.push(format!("data: {usage}\n\n"));
    }
    events.push("data: [DONE]\n\n".to_owned());
    events
}

fn openai_long_sse(model: &str) -> Vec<String> {
    let mut events: Vec<String> = (0..30)
        .map(|i| {
            let choices = json!([{"index": 0, "delta": {"content": format!("tok{i} ")}, "finish_reason": null}]);
            format!("data: {}\n\n", openai_chunk(model, &choices))
        })
        .collect();
    let mut usage = openai_chunk(model, &json!([]));
    usage["usage"] = openai_usage();
    events.push(format!("data: {usage}\n\n"));
    events.push("data: [DONE]\n\n".to_owned());
    events
}

fn anthropic_sse(model: &str) -> Vec<String> {
    let ev = |name: &str, v: &Value| format!("event: {name}\ndata: {v}\n\n");
    let mut events = vec![
        ev(
            "message_start",
            &json!({"type": "message_start", "message": {"id": "msg_baseline_fixed", "type": "message", "role": "assistant",
                "model": model, "content": [], "stop_reason": null, "stop_sequence": null, "usage": {"input_tokens": 10, "output_tokens": 1}}}),
        ),
        ev(
            "content_block_start",
            &json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        ),
    ];
    for piece in ["base", "line ", "answer"] {
        events.push(ev(
            "content_block_delta",
            &json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": piece}}),
        ));
    }
    events.push(ev(
        "content_block_stop",
        &json!({"type": "content_block_stop", "index": 0}),
    ));
    events.push(ev(
        "message_delta",
        &json!({"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null}, "usage": {"output_tokens": 20}}),
    ));
    events.push(ev("message_stop", &json!({"type": "message_stop"})));
    events
}

fn gemini_chunks() -> Vec<Value> {
    let pieces = ["base", "line ", "answer"];
    (1_u64..)
        .zip(pieces)
        .map(|(n, piece)| {
            let mut c = json!({
                "candidates": [{"content": {"role": "model", "parts": [{"text": piece}]}, "index": 0}],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": n * 6, "totalTokenCount": 10 + n * 6},
                "modelVersion": "gemini-baseline"
            });
            if n == 3 {
                c["candidates"][0]["finishReason"] = json!("STOP");
                c["usageMetadata"] =
                    json!({"promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30});
            }
            c
        })
        .collect()
}

fn gemini_sse() -> Vec<String> {
    gemini_chunks()
        .iter()
        .map(|c| format!("data: {c}\r\n\r\n"))
        .collect()
}

fn sse(record: Hits, path: String, events: Vec<String>) -> Response {
    let state = (events, 0_usize, record, path);
    let stream = futures_util::stream::unfold(state, |(events, i, record, path)| async move {
        let Some(chunk) = events.get(i).cloned() else {
            record.complete(path);
            return None;
        };
        if i > 0 {
            tokio::time::sleep(SSE_GAP).await;
        }
        Some((Ok::<_, Infallible>(chunk), (events, i + 1, record, path)))
    });
    (
        [(header::CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(stream),
    )
        .into_response()
}

fn json_response(v: &Value) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], v.to_string()).into_response()
}

fn handle(record: &Hits, uri: &Uri, body: &str) -> Response {
    let path = uri.path();
    let req: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let model = req.get("model").and_then(Value::as_str).unwrap_or("");
    let stream = req.get("stream").and_then(Value::as_bool).unwrap_or(false);
    lock(&record.hits).push(json!({
        "path": path,
        "query": uri.query(),
        "model": model,
        "stream": stream,
        "stream_options": req.get("stream_options").cloned().unwrap_or(Value::Null)
    }));
    let sse = |events| sse(Arc::clone(record), path.to_owned(), events);
    if path.starts_with("/fail") {
        let err = json!({"error": {"message": "scripted upstream 500", "type": "server_error"}});
        return (StatusCode::INTERNAL_SERVER_ERROR, json_response(&err)).into_response();
    }
    if path.starts_with("/openai-long") {
        return sse(openai_long_sse(model));
    }
    if path.starts_with("/anthropic") && stream {
        return sse(anthropic_sse(model));
    }
    if path.starts_with("/gemini") && path.contains("streamGenerateContent") {
        if uri.query().is_some_and(|q| q.contains("alt=sse")) {
            return sse(gemini_sse());
        }
        record.complete(path.to_owned());
        return json_response(&Value::Array(gemini_chunks()));
    }
    if path.starts_with("/openai-nousage") {
        return openai_reply(model, stream, false, sse);
    }
    if path.starts_with("/openai") {
        return openai_reply(model, stream, true, sse);
    }
    if path.starts_with("/anthropic") {
        return json_response(&json!({
            "id": "msg_baseline_fixed",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [{"type": "text", "text": "baseline answer"}],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {"input_tokens": 10, "output_tokens": 20}
        }));
    }
    if path.starts_with("/gemini") {
        return json_response(&json!({
            "candidates": [{"content": {"role": "model", "parts": [{"text": "baseline answer"}]}, "finishReason": "STOP", "index": 0}],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30},
            "modelVersion": "gemini-baseline"
        }));
    }
    (StatusCode::NOT_FOUND, "unscripted path").into_response()
}

fn openai_reply(
    model: &str,
    stream: bool,
    with_usage: bool,
    sse: impl FnOnce(Vec<String>) -> Response,
) -> Response {
    if stream {
        sse(openai_sse(model, with_usage))
    } else {
        json_response(&openai_ok(model, with_usage))
    }
}

pub(crate) async fn serve(listener: TcpListener, record: Hits) -> std::io::Result<()> {
    let app = axum::Router::new().fallback(move |uri: Uri, body: String| {
        let record = Arc::clone(&record);
        async move { handle(&record, &uri, &body) }
    });
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::{anthropic_sse, gemini_chunks, gemini_sse, openai_long_sse, openai_ok, openai_sse};
    use serde_json::Value;

    #[test]
    fn sse_ends_with_done_and_carries_usage_only_when_asked() {
        let with = openai_sse("m", true);
        let without = openai_sse("m", false);
        assert_eq!(with.last().map(String::as_str), Some("data: [DONE]\n\n"));
        assert_eq!(with.len(), without.len() + 1);
        assert!(with
            .iter()
            .all(|e| e.starts_with("data: ") && e.ends_with("\n\n")));
        assert!(with.iter().any(|e| e.contains("\"usage\"")));
        assert!(!without.iter().any(|e| e.contains("\"usage\"")));
    }

    #[test]
    fn non_stream_usage_is_optional() {
        assert_eq!(openai_ok("m", true)["usage"]["total_tokens"], 30);
        assert_eq!(openai_ok("m", false).get("usage"), None::<&Value>);
    }

    #[test]
    fn long_stream_has_thirty_content_chunks_then_usage() {
        let ev = openai_long_sse("m");
        assert_eq!(ev.len(), 32);
        assert!(ev[30].contains("\"usage\""));
        assert_eq!(ev[31], "data: [DONE]\n\n");
    }

    #[test]
    fn anthropic_stream_carries_input_and_output_usage() {
        let ev = anthropic_sse("m");
        assert!(
            ev[0].starts_with("event: message_start\n") && ev[0].contains("\"input_tokens\":10")
        );
        assert!(
            ev.iter()
                .any(|e| e.starts_with("event: message_delta\n")
                    && e.contains("\"output_tokens\":20"))
        );
        assert_eq!(
            ev.last().map(String::as_str),
            Some("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
        );
    }

    #[test]
    fn gemini_last_chunk_has_final_usage() {
        let chunks = gemini_chunks();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0]["usageMetadata"]["candidatesTokenCount"], 6);
        assert_eq!(chunks[1]["usageMetadata"]["totalTokenCount"], 22);
        assert_eq!(chunks[2]["usageMetadata"]["totalTokenCount"], 30);
        assert_eq!(chunks[2]["candidates"][0]["finishReason"], "STOP");
        assert!(gemini_sse().iter().all(|e| e.ends_with("\r\n\r\n")));
    }
}
