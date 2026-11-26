use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use tokio::net::TcpListener;

pub(crate) type Hits = Arc<Mutex<Vec<Value>>>;

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

fn sse(events: Vec<String>) -> Response {
    let stream = futures_util::stream::unfold((events, 0_usize), |(events, i)| async move {
        let chunk = events.get(i)?.clone();
        if i > 0 {
            tokio::time::sleep(SSE_GAP).await;
        }
        Some((Ok::<_, Infallible>(chunk), (events, i + 1)))
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

fn openai(model: &str, stream: bool, with_usage: bool) -> Response {
    if stream {
        sse(openai_sse(model, with_usage))
    } else {
        json_response(&openai_ok(model, with_usage))
    }
}

fn handle(hits: &Hits, uri: &Uri, body: &str) -> Response {
    let path = uri.path();
    let req: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let model = req.get("model").and_then(Value::as_str).unwrap_or("");
    let stream = req.get("stream").and_then(Value::as_bool).unwrap_or(false);
    hits.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(json!({
            "path": path,
            "model": model,
            "stream": stream,
            "stream_options": req.get("stream_options").cloned().unwrap_or(Value::Null)
        }));
    if path.starts_with("/fail") {
        let err = json!({"error": {"message": "scripted upstream 500", "type": "server_error"}});
        return (StatusCode::INTERNAL_SERVER_ERROR, json_response(&err)).into_response();
    }
    if path.starts_with("/openai-nousage") {
        return openai(model, stream, false);
    }
    if path.starts_with("/openai") {
        return openai(model, stream, true);
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

pub(crate) async fn serve(listener: TcpListener, hits: Hits) -> std::io::Result<()> {
    let app = axum::Router::new().fallback(move |uri: Uri, body: String| {
        let hits = Arc::clone(&hits);
        async move { handle(&hits, &uri, &body) }
    });
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::{openai_ok, openai_sse};
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
}
