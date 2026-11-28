# burncloud/router

BurnCloud 的模型中转。按 [burncloud/next](https://github.com/burncloud/next) 0.5.0 的契约实现，计费走 [burncloud/billing](https://github.com/burncloud/billing) 0.4.0。

## 结构

仓库根是 virtual workspace，成员在 `crates/` 下：

| crate | 内容 |
| --- | --- |
| `crates/relay-gateway` | 请求编排，实现 next 的 `runtime::ports::Gateway`：鉴权、限流、冻结、调用上游、边读边转发、结算。只依赖 next 的契约，没有 IO，执行器由调用方注入 |
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
- 非流式读完整个响应体后取顶层 `usage`，响应体是完整 JSON 时 `ended()` 为真。`feed` 只累积字节，`ended()` 每次调用都解析整个响应体，所以调用方只能在上游读完后调一次，`relay-gateway` 就是这样做的。
- 非流式响应体、流式 SSE 单行和单个事件 data 各有同一上限 `MAX_BUFFER_BYTES`（16 MiB）。超过时 `feed` 返回 `BufferLimitExceeded`，已缓冲的字节被丢弃，不会截断后再解析，之后的字节也不再累积；此时 `ended()` 为真，上游读完后按用量读不出处理。转发给客户端的响应体不受此上限影响。
- 用量字段集合固定：`prompt_tokens`、`completion_tokens`、`total_tokens`、`prompt_tokens_details.{cached_tokens,audio_tokens}`、`completion_tokens_details.{reasoning_tokens,audio_tokens,accepted_prediction_tokens,rejected_prediction_tokens}`。缺字段、多字段、不是 u64、`total_tokens` 不等于两项之和、缓存数大于输入、推理数大于输出、audio 或 prediction 不为 0，都返回错误，不按 0 处理。
- 换算：`input_tokens = prompt_tokens - cached_tokens`，`cached_input_tokens = cached_tokens`，`output_tokens = completion_tokens`。

IO 依赖由 `crates/relay-openai/bans.toml` 禁止，`cargo test` 会核对清单。

## relay-gateway

`RelayGateway::new` 接收一个实现 `Services` 的结构体，里面是 next 的各个端口（`ApiKeys`、`RateLimiter`、`Adapter`、`Upstream`、`Wallet`、`Clock`）和本 crate 定义的四个端口：

- `Spawn`：执行后台任务和重试等待，`relay-server` 用 tokio 实现。
- `PriceSource`：每个请求取一次价格快照，冻结和结算用同一份。
- `RequestIds`：请求没带 `Idempotency-Key` 时生成 `RequestId`。带了就直接用，要求 1 到 255 个可见 ASCII 字符。
- `Report`：`UsageUnreadable`、`UsageUnreadableRatio`、`SettlementCapped`（附带差额 `Shortfall`）、`MeterFailed` 和存储故障。首期由实现方写结构化日志，本 crate 不写日志、不开外部通道。

`handle` 的顺序：认证，读 key 限额，有 `requests_per_minute` 才限流，确定 `RequestId`，`prepare`，取价格快照并按上限计价，冻结。冻结额不是 USD 或为 0 时拒绝，不冻结。key 的花费限额转成 `KeyCap`，`Daily`、`Monthly` 从 UTC 自然日、自然月起点算。

冻结成功后，「调用上游、读完、结算」整段交给 `Spawn`，字节经有界通道交给 `Reply::body`。结局只有一个：

- 读到用量（包括上游没正常结束但已经给了用量）：按同一份快照计价，用这笔 `HoldId` `settle`。实际额超过冻结额或计价溢出时按冻结额结算，并报 `SettlementCapped`。
- 上游正常结束（`ByteStream` 读完且 `UsageMeter::ended` 为真）但读不出用量：不结算也不释放，冻结挂起等对账，报 `UsageUnreadable`。
- 上游没正常结束且没有用量、响应头之前失败、非 2xx：用这笔 `HoldId` `release`，不扣费。其中 meter 已在 `feed` 中报错过的，释放前记一条 `MeterFailed`。非 2xx 的响应体照常转发。

客户端丢弃 `Reply::body` 后停止转发，但继续读完上游并按上面的规则结算。`settle` 和 `release` 遇到存储失败按 `GatewayConfig::settle_attempts` 重试，用完仍失败就把冻结留给对账并报故障。重试时拿到 `AlreadySettled`（`settle`）或 `AlreadyReleased`（`release`）说明前一次其实已成功，按成功处理，不报故障。已经调过上游后不会因为内部错误去 `release`。`HoldMismatch` 和其它没有映射成 `Rejection` 的计费拒绝一样：报 `Stage::Settle` 或 `Stage::Release` 后返回，不扣费，也不改这笔冻结。

`hold` 成功时返回这一笔的 `HoldId`，之后的 `settle` 和 `release` 都带上它。`hold` 失败时没有令牌，不调用 `release`。存储错误先报 `Stage::Hold`，再向客户端返回 `GatewayError::Storage`，不调用上游。写入已经成功、只是确认丢了的冻结留给对账。

每个进入上游调用的请求，在结局确定时记一次已决：按用量结算、释放冻结、读不出用量而挂起，都算。只有正常结束但读不出用量、冻结保持挂起的那次同时记入读不出。`hold` 的存储错误发生在调用上游之前，不增加已决次数，也不记 `UsageUnreadable`。计数挂在 `RelayGateway` 上，从创建起累加，不按时间滑动，告警也不把计数清零。`GatewayConfig::unreadable_threshold` 默认是 `UnreadableThreshold::DEFAULT`：至少 100 次已决，且 `unreadable / decided` 大于等于 1/100。比较是 `unreadable * ratio_den >= decided * ratio_num`，两侧用 `u128`，不用浮点。`UnreadableThreshold::new` 在窗口为 0、分母为 0 或分子大于分母时返回错误。比例从低于阈值变成达到阈值时调用一次 `Report::usage_unreadable_ratio`，字段是 `decided`、`unreadable`、`window`、`ratio_num`、`ratio_den`、`at`。一直处于达到状态不重复调用；跌回阈值以下之后再次达到会再调用。边沿和计数在同一把锁里更新。

IO 依赖和 `relay-openai` 由 `crates/relay-gateway/bans.toml` 和清单测试禁止。

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
