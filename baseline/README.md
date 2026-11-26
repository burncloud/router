# 旧中转行为基线

对象：burncloud/burncloud @ `69aedf5867b072344427a3f78ec70a7524161755`。这个 SHA 只写在 `harness/Cargo.toml` 的 git 依赖 `rev` 里，`Cargo.lock` 跟着它；`run_twice.sh` 从 `cargo metadata` 读出它并校验是 40 位完整 SHA。

## 怎么跑

```
bash run_twice.sh           # debug 构建，全新 SQLite 各跑一次，比较两次结果，写 results/run1.json、run2.json、diff.txt
bash run_twice.sh --perf 200   # release 构建，跑 perf 探针，写 results/perf-release.json
python3 compare.py A.json B.json   # 只比较 compared 段，一致时退出码 0
```

工具链：仓库根的 `rust-toolchain.toml`（1.100.0）。需要 libssl-dev 和 python3（只用标准库）。

`harness/` 不在仓库的 Cargo workspace 里：它的 `Cargo.toml` 自带空的 `[workspace]`，CI 的 fmt、clippy、test、cargo-deny、cargo-vet 都不会碰它，旧仓库的依赖树（378 个 crate）也不会进仓库根的 `Cargo.lock` 和 `supply-chain/`。

旧夹具 `crates/traffic/router/tests/common.rs` 不入库。`run_twice.sh` 每次从 Cargo 按上面那个 rev 检出的源码里复制到 `harness/old/common.rs`（已被 `.gitignore` 忽略），`src/main.rs` 用 `#[path]` 引用它。`results/*.router.log` 是原始日志，每次重跑都会变（request_id、延迟），也不入库。

## 怎么搭的（与生产的差异如实列出）

**入口**：`burncloud_router::create_router_app`，并合并 internal_app。这和 interfaces/server 挂载的数据面是同一个 router。没有启动完整的 server 二进制，因为它依赖 Dioxus 客户端，构建很重；管理 API 和 node 不在中转范围内。

**数据库**：直接 include 旧仓库的 `crates/traffic/router/tests/common.rs`，用 `setup_db`、`insert_router_token`、`insert_test_channel` 和 `ensure_*` 建库。额外按生产启动顺序调用了 `UserDatabase::init`。数据库是 SQLite 临时文件，没有测 Postgres。

**渠道**：fixture 固定写 type=1。Anthropic 渠道改成 14，Gemini 渠道改成 24。

**价格**：在 router 启动前用 `BillingPriceModel::upsert` 写入，价格缓存只在启动时加载一次。输入价 2e9，输出价 8e9（纳美元 / 1M token）。上游 usage 固定为输入 10、输出 20，所以单次成本是 180000 纳美元。

**调度**：`SCHEDULER_POLICIES={"default":{"type":"passthrough"}}`，并按旧 `failover_tests.rs` 的规则选 session，让第一跳固定打到 500 渠道。生产默认策略没有覆盖到。

**假上游**：axum 进程内服务，按 base_url 前缀分派：
- `/openai`：带 usage
- `/openai-nousage`：不带 usage
- `/fail`：返回 500
- `/anthropic`
- `/gemini`

SSE 每个事件单独一块，块之间间隔 20 ms。

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

连跑两次，`compared` 段 826 行完全一致（`results/diff.txt`）。三轮 run-twice 都是这个结果。release 构建跑出的 `compared` 段和 debug 构建也完全一致。

`compare.py` 只比较 `compared` 段；`not_compared`（耗时、限流计数、启动日志）按设计不参与比较。

## perf 基线（release 构建，n=200，box 上单次测量，`results/perf-release.json`）

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

## 没覆盖或受限的部分

- Postgres、生产默认调度策略、完整 server 二进制、真实上游。
- Anthropic 和 Gemini 的流式。
- 客户端中途断开。
