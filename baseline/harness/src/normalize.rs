use serde_json::{json, Value};

const TIMESTAMP_KEYS: [&str; 3] = ["created", "created_at", "timestamp"];

fn is_uuid_char(c: char) -> bool {
    c.is_ascii_hexdigit() || c == '-'
}

fn is_uuidish(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&parts)
            .all(|(n, part)| part.len() == *n && part.chars().all(|c| c.is_ascii_hexdigit()))
}

pub(crate) fn norm_str(s: &str, ports: &[u16]) -> String {
    let mut out = s.to_owned();
    for p in ports {
        out = out.replace(&format!(":{p}"), ":<port>");
    }
    out.split_inclusive(|c: char| !is_uuid_char(c))
        .map(|tok| {
            let (core, tail) = match tok.char_indices().last() {
                Some((i, c)) if !is_uuid_char(c) => (&tok[..i], &tok[i..]),
                _ => (tok, ""),
            };
            if is_uuidish(core) {
                format!("<uuid>{tail}")
            } else {
                tok.to_owned()
            }
        })
        .collect()
}

pub(crate) fn norm_json(v: &Value, ports: &[u16]) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, x)| {
                    let nv = if TIMESTAMP_KEYS.contains(&k.as_str()) && !x.is_null() {
                        json!("<ts>")
                    } else {
                        norm_json(x, ports)
                    };
                    (k.clone(), nv)
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(|x| norm_json(x, ports)).collect()),
        Value::String(s) => Value::String(norm_str(s, ports)),
        Value::Null | Value::Bool(_) | Value::Number(_) => v.clone(),
    }
}

pub(crate) fn norm_body(raw: &str, ports: &[u16]) -> Value {
    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        return norm_json(&v, ports);
    }
    if raw.contains("data:") {
        let events: Vec<Value> = raw
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|d| {
                let d = d.trim();
                serde_json::from_str::<Value>(d)
                    .map_or_else(|_| json!(norm_str(d, ports)), |v| norm_json(&v, ports))
            })
            .collect();
        return json!({"sse_events": events});
    }
    json!({"raw_text": norm_str(raw, ports)})
}

pub(crate) fn norm_log_line(l: &str, ports: &[u16]) -> String {
    let mut out = String::new();
    let mut prev_digit = false;
    for c in norm_str(l, ports).chars() {
        if c.is_ascii_digit() {
            if !prev_digit {
                out.push('N');
            }
            prev_digit = true;
        } else {
            out.push(c);
            prev_digit = false;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{is_uuidish, norm_body, norm_json, norm_log_line, norm_str};
    use serde_json::json;

    #[test]
    fn uuid_shape_is_exact() {
        assert!(is_uuidish("1fcc5afd-1e52-4fbc-b587-eb312e8a8e04"));
        assert!(!is_uuidish("1fcc5afd-1e52-4fbc-b587-eb312e8a8e0"));
        assert!(!is_uuidish("chatcmpl-baseline-fixed"));
    }

    #[test]
    fn ports_and_uuids_are_replaced() {
        let s = "http://127.0.0.1:4321/x id=1fcc5afd-1e52-4fbc-b587-eb312e8a8e04, done";
        assert_eq!(
            norm_str(s, &[4321]),
            "http://127.0.0.1:<port>/x id=<uuid>, done"
        );
    }

    #[test]
    fn timestamps_are_masked_but_null_kept() {
        let v = json!({"created": 17, "created_at": null, "nested": [{"timestamp": "t"}], "n": 3});
        assert_eq!(
            norm_json(&v, &[]),
            json!({"created": "<ts>", "created_at": null, "nested": [{"timestamp": "<ts>"}], "n": 3})
        );
    }

    #[test]
    fn sse_and_raw_bodies_are_parsed() {
        let sse = "data: {\"created\": 1}\n\ndata: [DONE]\n\n";
        assert_eq!(
            norm_body(sse, &[]),
            json!({"sse_events": [{"created": "<ts>"}, "[DONE]"]})
        );
        assert_eq!(
            norm_body("plain :80", &[80]),
            json!({"raw_text": "plain :<port>"})
        );
    }

    #[test]
    fn log_digits_collapse_to_n() {
        assert_eq!(
            norm_log_line(" WARN x: 12 of 345 at :99", &[99]),
            " WARN x: N of N at :<port>"
        );
    }
}
