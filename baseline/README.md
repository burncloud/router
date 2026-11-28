# 旧中转行为基线

对象：burncloud/burncloud @ `69aedf5867b072344427a3f78ec70a7524161755`。这个 SHA 只写在 `harness/Cargo.toml` 的 git 依赖 `rev` 里，`Cargo.lock` 跟着它；`baseline-tool` 从 `cargo metadata` 读出它并校验是 40 位完整 SHA。

## 怎么跑

在仓库根目录：

```
cargo run --locked --manifest-path baseline/tool/Cargo.toml -- run-twice
cargo run --locked --manifest-path baseline/tool/Cargo.toml -- perf 200
cargo run --locked --manifest-path baseline/tool/Cargo.toml -- compare A.json B.json
```

- `run-twice`：debug 构建 harness，全新 SQLite 各跑一次，比较两次结果，写 `baseline/results/run1.json`、`run2.json`、`diff.txt`。
- `perf N`：release 构建 harness，跑 N 次 perf 探针，写 `baseline/results/perf-release.json`。
- `compare`：只比较 `compared` 段，一致时退出码 0，不一致时 1 并打印 unified diff，参数或文件错误时 2。`not_compared`（耗时、限流计数、启动日志）按设计不参与比较。

工具链：仓库根的 `rust-toolchain.toml`（1.100.0）。需要 libssl-dev。

`harness/` 和 `tool/` 都不在仓库的 Cargo workspace 里：各自的 `Cargo.toml` 自带空的 `[workspace]`，CI 的 fmt、clippy、test、cargo-deny、cargo-vet 都不会碰它们，旧仓库的依赖树（378 个 crate）也不会进仓库根的 `Cargo.lock` 和 `supply-chain/`。`tool/` 只依赖 anyhow 和 serde_json，不编译旧仓库，新中转可以直接用它的 `compare`。

旧夹具 `crates/traffic/router/tests/common.rs` 不入库。`baseline-tool` 每次从 Cargo 按上面那个 rev 检出的源码里复制到 `harness/old/common.rs`（已被 `.gitignore` 忽略），`harness/src/main.rs` 用 `#[path]` 引用它。

录制结果不入库：`baseline/results/` 整个目录被忽略。`compared` 段是确定的：本机三轮录制、搬运后用 `run-twice` 重跑，得到的都是同样的 1104 行；下面的表格和「实测的旧行为」就是那次录制的结论。原始日志 `results/*.router.log` 每次重跑都会变（request_id、延迟）。

## 怎么搭的（与完整部署的差异如实列出）

**入口**：`burncloud_router::create_router_app`，并合并 internal_app。这和 interfaces/server 挂载的数据面是同一个 router。没有启动完整的 server 二进制，因为它依赖 Dioxus 客户端，构建很重；管理 API 和 node 不在中转范围内。

**数据库**：直接 include 旧仓库的 `crates/traffic/router/tests/common.rs`，用 `setup_db`、`insert_router_token`、`insert_test_channel` 和 `ensure_*` 建库。额外按生产启动顺序调用了 `UserDatabase::init`。数据库是 SQLite 临时文件，没有测 Postgres。

**渠道**：fixture 固定写 type=1。Anthropic 渠道改成 14，Gemini 渠道改成 24。

**价格**：在 router 启动前用 `BillingPriceModel::upsert` 写入，价格缓存只在启动时加载一次。输入价 2e9，输出价 8e9（纳美元 / 1M token）。上游 usage 固定为输入 10、输出 20，所以单次成本是 180000 纳美元。

**调度**：`SCHEDULER_POLICIES={"default":{"type":"passthrough"}}`，并按旧 `failover_tests.rs` 的规则选 session，让第一跳固定打到 500 渠道。旧代码的默认策略没有覆盖到。

**假上游**：axum 进程内服务，按 base_url 前缀分派：
- `/openai`：带 usage
- `/openai-nousage`：不带 usage
- `/fail`：返回 500
- `/openai-long`：30 块的长流，末尾带 usage（断开场景用）
- `/anthropic`：`stream:true` 时回 Anthropic SSE（message_start 里 input_tokens，message_delta 里 output_tokens）
- `/gemini`：`streamGenerateContent` 带 `alt=sse` 时回 SSE（每块带 usageMetadata，末块是最终值）；不带 `alt=sse` 时按文档回一个 JSON 数组

SSE 每个事件单独一块，块之间间隔 20 ms。假上游记录每条流是否被读完（`upstream_streams_completed`）。

## 每个场景记录的内容

- 状态码、content-type、规范化后的响应体。SSE 逐事件解析，时间戳、UUID、端口都替换掉。
- 上游命中情况：路径、模型、是否流式、stream_options。
- 金额变化（纳美元）：`user_accounts.balance_usd`、`balance_cny`，`user_api_keys.used_quota`、`remain_quota`，`router_tokens.used_quota`。
- 新增的 router_logs 行（tokens、cost、cost_status、error_type、upstream_id）。
- 该场景期间 router 打出的 WARN 和 ERROR 日志（数字替换成 N）。完整日志在 `results/runN.router.log`（本地生成，不入库）。
- 首包时间和总延迟。这一项放在 `not_compared` 里，不参与比较。

限流场景只比较不受时间影响的量：
- 是否出现 429；
- 429 的响应体；
- 扣费是否等于成功次数乘以单价；
- 被限流的请求是否写了日志。

具体计数依赖墙钟，放在 `not_compared.nondeterministic` 里。

## 确定性

连跑两次，`compared` 段 1104 行完全一致（补录 3 个场景后；补录前是 826 行）。三轮 run-twice 都是这个结果。release 构建跑出的 `compared` 段和 debug 构建也完全一致。之后只按已定的计费规则改过 `expected_diff` 的说明文字，录到的旧行为没有变。

## perf 基线（release 构建，n=200，box 上单次测量，`perf 200`）

| 场景 | 首包 p50 | 首包 p95 | 总延迟 p50 | 总延迟 p95 |
|---|---|---|---|---|
| OpenAI 非流式 | 5.0 ms | 10.9 ms | 5.0 ms | 10.9 ms |
| OpenAI 流式（假上游 5 块，每块间隔 20 ms） | 3.4 ms | 4.7 ms | 109.0 ms | 111.2 ms |

这是单台机器上的单次测量，没有多轮取方差。新中转应该在同一台机器上、紧挨着旧系统重测，再做比较。

## 实测的旧行为（标「预期不同」的已写进 harness）

1. **流式请求扣 0。** 上游流末带了 usage，结果仍然是 tokens=0、cost=0、cost_status 为空。原因是 `lib.rs` 第 1954 行在 proxy_logic 返回时，也就是响应头刚到、流还没被消费时，就读了 token_counter。另外 router 不注入 `stream_options.include_usage`（上游收到的 stream_options 为 null），所以真实 OpenAI 也不会回 usage。
2. **钱包余额不参与计费。** 所有场景里 `balance_usd` 的变化都是 0。扣的是 key 的 `used_quota`：`deduct_quota` → `TokenService::deduct_quota` → `RouterTokenModel::deduct_quota`。钱包余额为 0、key 额度不限的用户可以正常调用，返回 200。
3. **非流式缺 usage 返回 200 且扣 0。** cost_status 为空，不是 `usage_missing`。旧 `common.rs` 的注释说质量闸门会拦成 502，实测不是这样。
4. **无价格模型被预检拒绝。** 返回 400 `model_not_found`，没有调用上游，也没有扣费，这部分是对的。但 router_logs 把它记成 `cost_status=upstream_error`、`error_type=router_reject`，分类有误。
5. **key 额度用完返回 402。** 错误码是 `insufficient_quota`，没有调用上游，也没有写 router_logs。
6. **限流。** 每个用户的令牌桶容量 100，连发 140 个请求时约 35 个返回 429 `rate_limit_exceeded`。被限流的请求不扣费，也不写 router_logs。
7. **故障转移。** 先打到 500 渠道，再切到健康渠道，返回 200，按健康渠道扣一次费。router_logs 只有一行，upstream_id 是健康渠道，500 那次尝试没有日志。
8. **上游路径。** OpenAI 渠道转发时会去掉客户端路径里的 `/v1`（base_url 应该自带 /v1）；Anthropic 和 Gemini 会保留完整路径。
9. **Anthropic 流式也扣 0。** 200、事件原样转发，但 tokens=0、cost=0，和第 1 条同因。
10. **Gemini 原生流式不可用。** 客户端按官方写法请求 `:streamGenerateContent?alt=sse`（body 里没有 `stream`），旧 router 转发时丢掉了 `alt=sse`，又按 `body.stream` 判断为非流式；上游回 JSON 数组后质量检查报 `No tokens found in Gemini response`，返回 502 `All upstreams failed`，不扣费，日志记 upstream_error。
11. **客户端中途断开。** 读到第一块就断开：旧 router 随之断开上游（30 块的上游流没被读完），扣 0，日志一行 200、cost 0。对比：旧系统扣 0；新系统按已定的产品规则，客户端断开后继续读完上游，按真实用量结算（上游自己中断、没有用量时释放冻结、不扣）。

## 没覆盖或受限的部分

- Postgres、旧代码默认调度策略、完整 server 二进制（旧中转没有上过生产，没有生产环境可比）。

## 已用真实上游确认的部分

- **OpenAI 流式 usage**：请求带 `stream_options.include_usage` 时，usage 在 `[DONE]` 前最后一个 `choices: []` 事件里，前面的事件带 `"usage": null`；不加时整条流没有 usage。非流式的 usage 和流末数值一致。
- **OpenAI 客户端断开**：用量后台已确认中途断开不计费，读完的才按完整回答计费。新系统的规则已定，不依赖这个结论：客户端断开后继续读完上游，按真实用量结算；上游自己中断、没有用量时释放冻结、不扣。
- **Gemini 流式**：`alt=sse`、不带 `alt` 和非流式都录过。`alt=sse` 的事件分隔符是 `\r\n\r\n`；每块都带 usageMetadata，数值累计，末块等于非流式。

## 需要真实上游样本才能定的部分

假上游的响应格式是按官方文档写的，下面几点必须用真实上游各录一次（脱敏后存成样本，喂给假上游回放）才能当验收依据：

1. **Anthropic 流式**：message_start / message_delta 中 usage 的真实字段，含 cache 相关 token 字段；以及中途断开时上游怎么计费。
2. **Gemini**：中途断开时上游怎么计费；usageMetadata 里 thoughts、cached 相关字段（已录样本里没有出现）。
3. **故障转移、限流在默认调度策略和 Postgres 下的行为**：本基线只在 SQLite + passthrough 策略下测过。旧中转没有上过生产，没有生产调度配置，改用旧代码默认策略加 Postgres 测试库，不需要真实上游。
