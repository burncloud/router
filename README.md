# burncloud/router

BurnCloud 的模型中转。按 [burncloud/next](https://github.com/burncloud/next) 0.3.0 的契约实现，计费走 [burncloud/billing](https://github.com/burncloud/billing)。

## 结构

仓库根是 virtual workspace，成员在 `crates/` 下：

| crate | 内容 |
| --- | --- |
| `crates/relay-openai` | OpenAI chat completions 协议的纯逻辑，实现 next 的 `router::ports::Adapter` 和 `UsageMeter`。没有 IO |

`baseline/harness`、`baseline/tool` 是旧中转的行为基线，各自是独立的 workspace，不在根 workspace 里，说明见 `baseline/README.md`。

## relay-openai

`OpenAiChat::new` 接收允许的模型和各自的最大输出 token 数。模型名按原样精确匹配，区分大小写，不做前缀或别名回退。

`prepare` 对请求体：

- 拒绝显式写了非 `default` 的 `service_tier`、`n` 不等于 1、`modalities` 含非 `text`、带 `audio`、`prediction` 或 `web_search_options`、消息里有非文本的 content part、请求的最大输出超过模型上限、`max_completion_tokens` 和 `max_tokens` 同时出现。每种拒绝都有固定的错误码（`PrepareError::code`）。
- 冻结上限：输入取请求体字节数，缓存命中取 0，输出依次取 `max_completion_tokens`、`max_tokens`、模型的最大输出。
- 发给上游的请求体只改两处：一律写入 `"service_tier": "default"`（原来没有这个字段时加在对象末尾，已有则就地改值）；流式请求把 `stream_options.include_usage` 设为 `true`（原来没有 `stream_options` 时加在对象末尾）。对象的 key 顺序（含嵌套对象，例如工具参数和 `response_format` 的 `json_schema` properties）保持原请求的顺序，不按字母重排。超出 `u64`/`i64` 的整数按数字原文透传，不会变成浮点。科学计数法会写成 `serde_json` 的规范形式（例如 `1e2` 变成 `1e+2`）。重新序列化后是紧凑 JSON，空白和 Unicode 转义会按 `serde_json` 的规则重写，不是整份请求体逐字节原样复制。

`UsageMeter`：

- 流式按 SSE 行在字节层面跨块切分，`\n` 和 `\r\n` 都认，不做有损解码。`"usage": null` 跳过，出现两次非空 usage、`[DONE]` 之后还有数据、上游的 error 事件、响应里 `service_tier` 不是 `default`，都返回错误。读到 `data: [DONE]` 加空行后 `ended()` 为真。
- 非流式读完整个响应体后取顶层 `usage`，响应体是完整 JSON 时 `ended()` 为真。
- 用量字段集合固定：`prompt_tokens`、`completion_tokens`、`total_tokens`、`prompt_tokens_details.{cached_tokens,audio_tokens}`、`completion_tokens_details.{reasoning_tokens,audio_tokens,accepted_prediction_tokens,rejected_prediction_tokens}`。缺字段、多字段、不是 u64、`total_tokens` 不等于两项之和、缓存数大于输入、推理数大于输出、audio 或 prediction 不为 0，都返回错误，不按 0 处理。
- 换算：`input_tokens = prompt_tokens - cached_tokens`，`cached_input_tokens = cached_tokens`，`output_tokens = completion_tokens`。

IO 依赖由 `crates/relay-openai/bans.toml` 禁止，`cargo test` 会核对清单。

## 本地复现

需要 `rust-toolchain.toml` 里的 `1.100.0`（含 `clippy` 和 `rustfmt`）。在仓库根执行：

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic
PROPTEST_CASES=256 cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features
cargo vet --locked
```

CI 调用 [burncloud/ci](https://github.com/burncloud/ci) 的共享工作流 `rust.yml@latest`。`.github/`、`supply-chain/`、`rust-toolchain.toml` 和各 `Cargo.toml` 的 `lints` 是受保护路径，改动需要管理员在 pull request 里 bypass。
