mod support;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use billing::errors::BillingRejection;
use billing::model::KeyCap;
use identity::model::{KeyLimit, LimitPeriod, RequestsPerMinute, SpendLimit};
use kernel::model::{AccountId, Currency, KeyId, Money, RequestId, UnixMillis};
use relay_gateway::{
    since, BodyInterrupted, Fault, GatewayConfig, GatewayError, MeterFailed, RelayGateway,
    ReplyBody, Shortfall, Stage, UnreadableThreshold, MAX_IDEMPOTENCY_KEY_LEN,
};
use router::ports::ByteStream;
use runtime::errors::Rejection;
use runtime::events::{ActualCost, SettlementCapped, UsageUnreadable};
use runtime::model::Reply;
use runtime::ports::Gateway;
use support::{
    block_on, chunk, config, gpt_price, lock, stack, stack_with, Event, Hold, HoldState,
    PriceError, Script, Stack, Step, Unsupported, ACCOUNT, BALANCE, KEY, KEY_ID, MAX_OUTPUT, NOW,
};

type Outcome = Result<Reply<ReplyBody>, GatewayError<Unsupported, PriceError>>;

const HELD: u64 = 3 * 2 + MAX_OUTPUT * 10;

fn usd(minor_units: u64) -> Money {
    Money {
        currency: Currency::Usd,
        minor_units,
    }
}

fn call(gateway: &RelayGateway<Stack>, idempotency_key: Option<&str>, body: &str) -> Outcome {
    block_on(gateway.handle(KEY, idempotency_key, body.as_bytes()))
}

fn drain(body: &mut ReplyBody) -> (Vec<Vec<u8>>, bool) {
    let mut chunks = Vec::new();
    loop {
        assert!(chunks.len() <= 64, "body never ends");
        match block_on(body.next_chunk()) {
            Some(Ok(bytes)) => chunks.push(bytes),
            Some(Err(BodyInterrupted)) => return (chunks, true),
            None => return (chunks, false),
        }
    }
}

fn texts(chunks: &[Vec<u8>]) -> Vec<String> {
    chunks
        .iter()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .collect()
}

fn run(gateway: &RelayGateway<Stack>, script: Script) -> (Reply<()>, Vec<String>, bool) {
    gateway.services().upstream.push(script);
    let Ok(mut reply) = call(gateway, None, "gpt") else {
        panic!("request failed");
    };
    let (chunks, interrupted) = drain(&mut reply.body);
    let head = Reply {
        status: reply.status,
        streaming: reply.streaming,
        body: (),
    };
    drop(reply);
    gateway.services().threads.join();
    (head, texts(&chunks), interrupted)
}

fn streaming(gateway: &RelayGateway<Stack>) {
    gateway
        .services()
        .adapter
        .streaming
        .store(true, Ordering::SeqCst);
}

fn holds(gateway: &RelayGateway<Stack>) -> Vec<(RequestId, Hold)> {
    gateway.services().wallet.holds()
}

fn one_hold(gateway: &RelayGateway<Stack>, state: HoldState) {
    assert_eq!(
        holds(gateway),
        [(
            RequestId("gen-1".into()),
            Hold {
                amount: usd(HELD),
                state,
            }
        )]
    );
}

fn calls(gateway: &RelayGateway<Stack>) -> (usize, usize) {
    let wallet = &gateway.services().wallet;
    (
        wallet.settle_calls.load(Ordering::SeqCst),
        wallet.release_calls.load(Ordering::SeqCst),
    )
}

fn released_without_charge(gateway: &RelayGateway<Stack>) {
    one_hold(gateway, HoldState::Released);
    assert_eq!(calls(gateway), (0, 1));
    assert_eq!(gateway.services().wallet.available(ACCOUNT), BALANCE);
    assert_eq!(gateway.services().wallet.revenue(), 0);
    gateway.services().wallet.check_books();
}

fn settled(gateway: &RelayGateway<Stack>, amount: u64) {
    one_hold(gateway, HoldState::Settled(amount));
    assert_eq!(calls(gateway), (1, 0));
    assert_eq!(
        gateway.services().wallet.available(ACCOUNT),
        BALANCE - amount
    );
    assert_eq!(gateway.services().wallet.revenue(), amount);
    gateway.services().wallet.check_books();
}

fn principal_fields() -> (RequestId, AccountId, KeyId) {
    (
        RequestId("gen-1".into()),
        AccountId(ACCOUNT.into()),
        KeyId(KEY_ID.into()),
    )
}

#[test]
fn debug_hides_services() {
    assert_eq!(format!("{:?}", stack()), "RelayGateway { .. }");
}

#[test]
fn success_holds_the_limit_and_settles_actual_usage() {
    let gateway = stack();
    streaming(&gateway);
    let (reply, chunks, interrupted) = run(
        &gateway,
        Script::Reply(200, vec![chunk("a"), chunk("usage:5,1,20"), chunk("done")]),
    );
    assert_eq!((reply.status, reply.streaming), (200, true));
    assert_eq!(chunks, ["a", "usage:5,1,20", "done"]);
    assert!(!interrupted);
    settled(&gateway, gpt_price(5, 1, 20));
    let services = gateway.services();
    assert_eq!(
        *lock(&services.upstream.bodies),
        [(b"up:gpt".to_vec(), true)]
    );
    assert_eq!(services.report.events(), []);
    assert_eq!(*lock(&services.wallet.caps), [None]);
    assert_eq!(*lock(&services.limiter.calls), []);
    assert_eq!(services.prices.loads.load(Ordering::SeqCst), 1);
    assert_eq!(services.threads.sleeps(), []);
}

fn rejected(
    gateway: &RelayGateway<Stack>,
    key: &str,
    idempotency: Option<&str>,
    body: &str,
) -> GatewayError<Unsupported, PriceError> {
    let services = gateway.services();
    let before = (
        services.upstream.calls(),
        holds(gateway),
        services.wallet.available(ACCOUNT),
        calls(gateway),
    );
    services.upstream.push(Script::Reply(
        200,
        vec![chunk("usage:1,0,1"), chunk("done")],
    ));
    let outcome = block_on(gateway.handle(key, idempotency, body.as_bytes()));
    let Err(error) = outcome else {
        panic!("request was accepted");
    };
    services.threads.join();
    lock(&services.upstream.scripts).clear();
    let after = (
        services.upstream.calls(),
        holds(gateway),
        services.wallet.available(ACCOUNT),
        calls(gateway),
    );
    assert_eq!(after, before);
    error
}

fn limited(spend: Option<SpendLimit>, per_minute: Option<u32>) -> RelayGateway<Stack> {
    stack_with(
        Some(KeyLimit {
            spend,
            requests_per_minute: per_minute.map(RequestsPerMinute),
        }),
        config(3),
    )
}

#[test]
fn rejections_never_reach_upstream_or_hold() {
    let gateway = stack();
    assert_eq!(
        rejected(&gateway, "sk-other", None, "gpt"),
        GatewayError::Rejected(Rejection::Unauthenticated)
    );
    assert_eq!(
        rejected(&stack_with(None, config(3)), KEY, None, "gpt"),
        GatewayError::Rejected(Rejection::UnknownKey)
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "nope"),
        GatewayError::Price(PriceError::UnknownModel)
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "reject"),
        GatewayError::Prepare(Unsupported)
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "huge"),
        GatewayError::Rejected(Rejection::InsufficientBalance)
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "free"),
        GatewayError::ZeroPrice
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "yuan"),
        GatewayError::UnsupportedCurrency
    );
    assert_eq!(gateway.services().keys.calls.load(Ordering::SeqCst), 6);
}

#[test]
fn throttled_keys_are_rejected() {
    let gateway = limited(None, Some(5));
    gateway
        .services()
        .limiter
        .throttle
        .store(true, Ordering::SeqCst);
    assert_eq!(
        rejected(&gateway, KEY, None, "gpt"),
        GatewayError::Rejected(Rejection::RateLimited)
    );
    assert_eq!(
        *lock(&gateway.services().limiter.calls),
        [(KeyId(KEY_ID.into()), RequestsPerMinute(5), UnixMillis(NOW))]
    );
}

#[test]
fn admitted_keys_pass_the_limiter() {
    let gateway = limited(None, Some(5));
    run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:1,0,1"), chunk("done")]),
    );
    assert_eq!(
        gateway
            .services()
            .limiter
            .calls
            .lock()
            .map(|calls| calls.len())
            .ok(),
        Some(1)
    );
    settled(&gateway, gpt_price(1, 0, 1));
}

#[test]
fn spend_limits_become_key_caps() {
    let gateway = limited(
        Some(SpendLimit {
            amount: usd(HELD - 1),
            period: LimitPeriod::Daily,
        }),
        None,
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "gpt"),
        GatewayError::Rejected(Rejection::LimitExceeded)
    );
    assert_eq!(
        *lock(&gateway.services().wallet.caps),
        [Some(KeyCap {
            since: since(LimitPeriod::Daily, UnixMillis(NOW)),
            limit: usd(HELD - 1),
        })]
    );
    let gateway = limited(
        Some(SpendLimit {
            amount: Money {
                currency: Currency::Cny,
                minor_units: u64::MAX,
            },
            period: LimitPeriod::Lifetime,
        }),
        None,
    );
    assert_eq!(
        rejected(&gateway, KEY, None, "gpt"),
        GatewayError::Rejected(Rejection::CurrencyMismatch)
    );
    let gateway = limited(
        Some(SpendLimit {
            amount: usd(HELD),
            period: LimitPeriod::Monthly,
        }),
        None,
    );
    run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:1,0,1"), chunk("done")]),
    );
    settled(&gateway, gpt_price(1, 0, 1));
}

#[test]
fn storage_failures_before_upstream_are_errors() {
    let gateway = stack();
    gateway.services().keys.broken.store(true, Ordering::SeqCst);
    assert_eq!(rejected(&gateway, KEY, None, "gpt"), GatewayError::Identity);
    let gateway = stack();
    gateway
        .services()
        .prices
        .broken
        .store(true, Ordering::SeqCst);
    assert_eq!(
        rejected(&gateway, KEY, None, "gpt"),
        GatewayError::PricesUnavailable
    );
}

fn client_storage(gateway: &RelayGateway<Stack>) -> Vec<Fault> {
    gateway.services().upstream.push(Script::Reply(
        200,
        vec![chunk("usage:1,0,1"), chunk("done")],
    ));
    let Err(error) = call(gateway, None, "gpt") else {
        panic!("request was accepted");
    };
    gateway.services().threads.join();
    lock(&gateway.services().upstream.scripts).clear();
    assert_eq!(error, GatewayError::Storage);
    assert_eq!(gateway.services().upstream.calls(), 0);
    let mut faults = Vec::new();
    for event in gateway.services().report.events() {
        match event {
            Event::Fault(fault) => faults.push(fault),
            Event::Unreadable(_) | Event::Ratio(_) | Event::Capped(..) | Event::MeterFailed(_) => {
                panic!("unexpected report");
            }
        }
    }
    faults
}

fn hold_storage_fault() -> Fault {
    Fault {
        stage: Stage::Hold,
        request: None,
        account: Some(AccountId(ACCOUNT.into())),
        attempts: 1,
        detail: "Storage(StoreDown)".to_owned(),
    }
}

#[test]
fn hold_lost_ack_stays_open_without_release() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .hold_lost_acks
        .store(1, Ordering::SeqCst);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    one_hold(&gateway, HoldState::Open);
    assert_eq!(calls(&gateway), (0, 0));
    assert_eq!(gateway.services().wallet.available(ACCOUNT), BALANCE - HELD);
    assert_eq!(gateway.services().wallet.revenue(), 0);
    assert_eq!(gateway.services().threads.sleeps(), []);
    gateway.services().wallet.check_books();
}

#[test]
fn hold_storage_then_missing_hold_is_not_a_release_fault() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .hold_fault
        .store(true, Ordering::SeqCst);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    assert_eq!(holds(&gateway), []);
    assert_eq!(calls(&gateway), (0, 0));
    assert_eq!(gateway.services().wallet.available(ACCOUNT), BALANCE);
    assert_eq!(gateway.services().wallet.revenue(), 0);
    gateway.services().wallet.check_books();
}

#[test]
fn hold_storage_then_already_released_is_not_a_release_fault() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .hold_fault
        .store(true, Ordering::SeqCst);
    *lock(&gateway.services().wallet.release_rejection) = Some(BillingRejection::AlreadyReleased);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    assert_eq!(holds(&gateway), []);
    assert_eq!(calls(&gateway), (0, 0));
    assert_eq!(gateway.services().threads.sleeps(), []);
    gateway.services().wallet.check_books();
}

#[test]
fn hold_storage_does_not_release_on_a_staged_rejection() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .hold_fault
        .store(true, Ordering::SeqCst);
    *lock(&gateway.services().wallet.release_rejection) = Some(BillingRejection::AlreadySettled);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    assert_eq!(calls(&gateway), (0, 0));
    assert_eq!(gateway.services().threads.sleeps(), []);
    gateway.services().wallet.check_books();
}

#[test]
fn hold_lost_ack_does_not_retry_release() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .hold_lost_acks
        .store(1, Ordering::SeqCst);
    gateway
        .services()
        .wallet
        .release_faults
        .store(2, Ordering::SeqCst);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    one_hold(&gateway, HoldState::Open);
    assert_eq!(calls(&gateway), (0, 0));
    assert_eq!(gateway.services().threads.sleeps(), []);
    assert_eq!(gateway.services().wallet.revenue(), 0);
    gateway.services().wallet.check_books();
}

#[test]
fn hold_lost_ack_stays_open_when_release_would_fail() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .hold_lost_acks
        .store(1, Ordering::SeqCst);
    gateway
        .services()
        .wallet
        .release_faults
        .store(5, Ordering::SeqCst);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    one_hold(&gateway, HoldState::Open);
    assert_eq!(calls(&gateway), (0, 0));
    assert_eq!(gateway.services().threads.sleeps().len(), 0);
    assert_eq!(gateway.services().wallet.available(ACCOUNT), BALANCE - HELD);
    assert_eq!(gateway.services().wallet.revenue(), 0);
    gateway.services().wallet.check_books();
}

#[test]
fn hold_storage_does_not_count_as_decided() {
    let Ok(threshold) = UnreadableThreshold::new(1, 1, 1) else {
        panic!("threshold");
    };
    let gateway = stack_with(
        Some(KeyLimit {
            spend: None,
            requests_per_minute: None,
        }),
        GatewayConfig {
            unreadable_threshold: threshold,
            ..config(3)
        },
    );
    gateway
        .services()
        .wallet
        .hold_fault
        .store(true, Ordering::SeqCst);
    assert_eq!(client_storage(&gateway), [hold_storage_fault()]);
    gateway
        .services()
        .wallet
        .hold_fault
        .store(false, Ordering::SeqCst);
    run(&gateway, Script::Reply(200, vec![chunk("done")]));
    let ratios: Vec<_> = gateway
        .services()
        .report
        .events()
        .into_iter()
        .filter_map(|event| match event {
            Event::Ratio(event) => Some((event.decided, event.unreadable)),
            Event::Fault(_) | Event::Unreadable(_) | Event::Capped(..) | Event::MeterFailed(_) => {
                None
            }
        })
        .collect();
    assert_eq!(ratios, [(1, 1)]);
}

#[test]
fn idempotency_keys_are_checked_and_used() {
    let gateway = stack();
    for bad in ["", "has space", "tab\t", "中文"] {
        assert_eq!(
            rejected(&gateway, KEY, Some(bad), "gpt"),
            GatewayError::InvalidIdempotencyKey
        );
    }
    let long = "k".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1);
    assert_eq!(
        rejected(&gateway, KEY, Some(&long), "gpt"),
        GatewayError::InvalidIdempotencyKey
    );
    let longest = "k".repeat(MAX_IDEMPOTENCY_KEY_LEN);
    gateway.services().upstream.push(Script::Reply(
        200,
        vec![chunk("usage:1,0,1"), chunk("done")],
    ));
    let Ok(mut reply) = call(&gateway, Some(&longest), "gpt") else {
        panic!("longest key was rejected");
    };
    drain(&mut reply.body);
    drop(reply);
    gateway.services().threads.join();
    assert_eq!(holds(&gateway)[0].0, RequestId(longest.clone()));
    assert_eq!(
        rejected(&gateway, KEY, Some(&longest), "gpt"),
        GatewayError::Rejected(Rejection::DuplicateRequest)
    );
}

#[test]
fn failure_before_headers_releases() {
    let gateway = stack();
    gateway.services().upstream.push(Script::Fail);
    assert!(matches!(
        call(&gateway, None, "gpt"),
        Err(GatewayError::Upstream)
    ));
    gateway.services().threads.join();
    released_without_charge(&gateway);
    let Some(Event::Fault(fault)) = gateway.services().report.events().pop() else {
        panic!("no fault recorded");
    };
    assert_eq!(fault.stage, Stage::Upstream);
    assert_eq!(fault.request, Some(RequestId("gen-1".into())));
}

#[test]
fn broken_stream_without_usage_releases() {
    let gateway = stack();
    streaming(&gateway);
    let (_, chunks, interrupted) = run(&gateway, Script::Reply(200, vec![chunk("a"), Step::Break]));
    assert_eq!(chunks, ["a"]);
    assert!(interrupted);
    released_without_charge(&gateway);
    assert_eq!(gateway.services().report.events(), []);
}

#[test]
fn eof_without_done_or_usage_releases() {
    let gateway = stack();
    streaming(&gateway);
    let (_, chunks, interrupted) = run(&gateway, Script::Reply(200, vec![chunk("a")]));
    assert_eq!(chunks, ["a"]);
    assert!(!interrupted);
    released_without_charge(&gateway);
    assert_eq!(gateway.services().report.events(), []);
}

fn released_after_meter_failure(gateway: &RelayGateway<Stack>) {
    released_without_charge(gateway);
    let (request, account, key) = principal_fields();
    assert_eq!(
        gateway.services().report.events(),
        [Event::MeterFailed(MeterFailed {
            request,
            account,
            key,
            held: usd(HELD),
            at: UnixMillis(NOW),
        })]
    );
}

#[test]
fn meter_failure_then_broken_stream_releases_and_reports() {
    let gateway = stack();
    streaming(&gateway);
    let (_, chunks, interrupted) = run(
        &gateway,
        Script::Reply(200, vec![chunk("a"), chunk("garbled"), Step::Break]),
    );
    assert_eq!(chunks, ["a", "garbled"]);
    assert!(interrupted);
    released_after_meter_failure(&gateway);
}

#[test]
fn meter_failure_then_eof_without_done_releases_and_reports() {
    let gateway = stack();
    streaming(&gateway);
    let (_, chunks, interrupted) = run(
        &gateway,
        Script::Reply(200, vec![chunk("a"), chunk("garbled")]),
    );
    assert_eq!(chunks, ["a", "garbled"]);
    assert!(!interrupted);
    released_after_meter_failure(&gateway);
}

#[test]
fn duplicate_usage_then_broken_stream_releases_and_reports() {
    let gateway = stack();
    streaming(&gateway);
    let script = vec![chunk("usage:5,1,20"), chunk("usage:5,1,20"), Step::Break];
    let (_, chunks, interrupted) = run(&gateway, Script::Reply(200, script));
    assert_eq!(chunks, ["usage:5,1,20", "usage:5,1,20"]);
    assert!(interrupted);
    released_after_meter_failure(&gateway);
}

#[test]
fn usage_without_done_still_settles() {
    let gateway = stack();
    streaming(&gateway);
    let (_, _, interrupted) = run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:5,1,20"), Step::Break]),
    );
    assert!(interrupted);
    settled(&gateway, gpt_price(5, 1, 20));
    let gateway = stack();
    run(&gateway, Script::Reply(200, vec![chunk("usage:5,1,20")]));
    settled(&gateway, gpt_price(5, 1, 20));
}

fn unreadable(gateway: &RelayGateway<Stack>) {
    one_hold(gateway, HoldState::Open);
    assert_eq!(calls(gateway), (0, 0));
    assert_eq!(gateway.services().wallet.available(ACCOUNT), BALANCE - HELD);
    assert_eq!(gateway.services().wallet.revenue(), 0);
    gateway.services().wallet.check_books();
    let (request, account, key) = principal_fields();
    assert_eq!(
        gateway.services().report.events(),
        [Event::Unreadable(UsageUnreadable {
            request,
            account,
            key,
            held: usd(HELD),
            at: UnixMillis(NOW),
        })]
    );
}

#[test]
fn normal_end_without_usage_stays_pending() {
    let gateway = stack();
    streaming(&gateway);
    let (reply, chunks, _) = run(
        &gateway,
        Script::Reply(200, vec![chunk("a"), chunk("done")]),
    );
    assert!(reply.streaming);
    assert_eq!(chunks, ["a", "done"]);
    unreadable(&gateway);
    let gateway = stack();
    let (reply, chunks, _) = run(&gateway, Script::Reply(200, vec![chunk("done")]));
    assert!(!reply.streaming);
    assert_eq!(chunks, ["done"]);
    assert_eq!(
        *lock(&gateway.services().upstream.bodies),
        [(b"up:gpt".to_vec(), false)]
    );
    unreadable(&gateway);
}

#[test]
fn meter_failure_mid_stream_stays_pending() {
    let gateway = stack();
    streaming(&gateway);
    let script = vec![
        chunk("a"),
        chunk("garbled"),
        chunk("usage:5,1,20"),
        chunk("b"),
        chunk("done"),
    ];
    let (_, chunks, interrupted) = run(&gateway, Script::Reply(200, script));
    assert_eq!(chunks, ["a", "garbled", "usage:5,1,20", "b", "done"]);
    assert!(!interrupted);
    assert_eq!(gateway.services().upstream.read.load(Ordering::SeqCst), 5);
    unreadable(&gateway);
}

#[test]
fn client_disconnect_keeps_reading_and_settles() {
    let gateway = stack();
    streaming(&gateway);
    let mut script: Vec<Step> = (0..28).map(|index| chunk(&format!("c{index}"))).collect();
    script.push(chunk("usage:5,1,20"));
    script.push(chunk("done"));
    gateway.services().upstream.push(Script::Reply(200, script));
    let Ok(mut reply) = call(&gateway, None, "gpt") else {
        panic!("request failed");
    };
    assert_eq!(block_on(reply.body.next_chunk()), Some(Ok(b"c0".to_vec())));
    drop(reply);
    gateway.services().threads.join();
    let upstream = &gateway.services().upstream;
    assert_eq!(upstream.read.load(Ordering::SeqCst), 30);
    assert!(upstream.exhausted.load(Ordering::SeqCst));
    settled(&gateway, gpt_price(5, 1, 20));
}

#[test]
fn client_disconnect_before_headers_still_settles() {
    let gateway = stack();
    gateway.services().upstream.push(Script::Reply(
        200,
        vec![chunk("usage:5,1,20"), chunk("done")],
    ));
    let Ok(reply) = call(&gateway, None, "gpt") else {
        panic!("request failed");
    };
    drop(reply);
    gateway.services().threads.join();
    settled(&gateway, gpt_price(5, 1, 20));
}

#[test]
fn cost_above_the_hold_is_capped() {
    let gateway = stack();
    run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:5,1,2000"), chunk("done")]),
    );
    settled(&gateway, HELD);
    let actual = gpt_price(5, 1, 2_000);
    let (request, account, key) = principal_fields();
    assert_eq!(
        gateway.services().report.events(),
        [Event::Capped(
            SettlementCapped {
                request,
                account,
                key,
                held: usd(HELD),
                actual: ActualCost::Priced(usd(actual)),
                at: UnixMillis(NOW),
            },
            Shortfall::Minor(actual - HELD),
        )]
    );
}

#[test]
fn cost_equal_to_the_hold_is_not_capped() {
    let gateway = stack();
    run(
        &gateway,
        Script::Reply(
            200,
            vec![chunk(&format!("usage:3,0,{MAX_OUTPUT}")), chunk("done")],
        ),
    );
    settled(&gateway, HELD);
    assert_eq!(gateway.services().report.events(), []);
}

#[test]
fn pricing_overflow_is_capped() {
    let gateway = stack();
    gateway.services().wallet.deposit(ACCOUNT, u64::MAX / 2);
    gateway.services().upstream.push(Script::Reply(
        200,
        vec![chunk("usage:1,0,3000"), chunk("done")],
    ));
    let Ok(mut reply) = call(&gateway, None, "huge") else {
        panic!("request failed");
    };
    drain(&mut reply.body);
    drop(reply);
    gateway.services().threads.join();
    let holds = holds(&gateway);
    let [(_, hold)] = holds.as_slice() else {
        panic!("expected one hold");
    };
    assert_eq!(hold.state, HoldState::Settled(hold.amount.minor_units));
    let events = gateway.services().report.events();
    let [Event::Capped(event, Shortfall::Overflow)] = events.as_slice() else {
        panic!("expected a capped settlement");
    };
    assert_eq!(event.actual, ActualCost::Overflow);
    assert_eq!(event.held, hold.amount);
}

#[test]
fn concurrent_duplicates_call_upstream_once() {
    let gateway = stack();
    gateway.services().upstream.push(Script::Reply(
        200,
        vec![chunk("usage:1,0,1"), chunk("done")],
    ));
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let gateway = gateway.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                match call(&gateway, Some("same-request"), "gpt") {
                    Ok(mut reply) => {
                        drain(&mut reply.body);
                        None
                    }
                    Err(error) => Some(error),
                }
            })
        })
        .collect();
    let mut errors = Vec::new();
    for worker in workers {
        let Ok(result) = worker.join() else {
            panic!("worker panicked");
        };
        errors.extend(result);
    }
    gateway.services().threads.join();
    assert_eq!(
        errors,
        [GatewayError::Rejected(Rejection::DuplicateRequest)]
    );
    assert_eq!(gateway.services().upstream.calls(), 1);
    assert_eq!(holds(&gateway).len(), 1);
}

#[test]
fn same_key_storage_failure_leaves_the_first_hold_to_settle() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .holds_before_storage_fault
        .store(1, Ordering::SeqCst);
    gateway.services().upstream.push(Script::Reply(
        200,
        vec![chunk("usage:1,0,1"), chunk("done")],
    ));
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let gateway = gateway.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                match call(&gateway, Some("same-request"), "gpt") {
                    Ok(mut reply) => {
                        drain(&mut reply.body);
                        None
                    }
                    Err(error) => Some(error),
                }
            })
        })
        .collect();
    let mut errors = Vec::new();
    for worker in workers {
        let Ok(result) = worker.join() else {
            panic!("worker panicked");
        };
        errors.extend(result);
    }
    gateway.services().threads.join();
    assert_eq!(errors, [GatewayError::Storage]);
    assert_eq!(calls(&gateway), (1, 0));
    assert_eq!(gateway.services().wallet.revenue(), gpt_price(1, 0, 1));
    match holds(&gateway).as_slice() {
        [(_, hold)] => assert_eq!(hold.state, HoldState::Settled(gpt_price(1, 0, 1))),
        _ => panic!("expected one hold"),
    }
    assert_eq!(
        gateway.services().report.events(),
        [Event::Fault(hold_storage_fault())]
    );
    gateway.services().wallet.check_books();
}

#[test]
fn hold_mismatch_does_not_charge_or_release() {
    let cases = [
        (
            Script::Reply(200, vec![chunk("usage:1,0,1"), chunk("done")]),
            (1, 0),
            Stage::Settle,
        ),
        (
            Script::Reply(200, vec![chunk("a"), Step::Break]),
            (0, 1),
            Stage::Release,
        ),
    ];
    for (script, expected_calls, stage) in cases {
        let gateway = stack();
        gateway
            .services()
            .wallet
            .foreign_hold_id
            .store(true, Ordering::SeqCst);
        run(&gateway, script);
        assert_eq!(calls(&gateway), expected_calls);
        one_hold(&gateway, HoldState::Open);
        assert_eq!(gateway.services().wallet.revenue(), 0);
        let [Event::Fault(fault)] = &gateway.services().report.events()[..] else {
            panic!("expected one fault");
        };
        assert_eq!(fault.stage, stage);
        assert_eq!(fault.detail, "HoldMismatch");
        gateway.services().wallet.check_books();
    }
}

#[test]
fn settle_storage_faults_are_retried() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .settle_faults
        .store(2, Ordering::SeqCst);
    run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:5,1,20"), chunk("done")]),
    );
    one_hold(&gateway, HoldState::Settled(gpt_price(5, 1, 20)));
    assert_eq!(calls(&gateway), (3, 0));
    assert_eq!(
        gateway.services().threads.sleeps(),
        [Duration::from_millis(7), Duration::from_millis(7)]
    );
    assert_eq!(gateway.services().report.events(), []);
}

#[test]
fn settle_faults_after_retries_leave_the_hold() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .settle_faults
        .store(5, Ordering::SeqCst);
    run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:5,1,20"), chunk("done")]),
    );
    one_hold(&gateway, HoldState::Open);
    assert_eq!(calls(&gateway), (3, 0));
    let Some(Event::Fault(fault)) = gateway.services().report.events().pop() else {
        panic!("no fault recorded");
    };
    assert_eq!(fault.stage, Stage::Settle);
    assert_eq!(fault.attempts, 3);
    assert_eq!(fault.account, Some(AccountId(ACCOUNT.into())));
    gateway.services().wallet.check_books();
}

#[test]
fn release_faults_are_retried() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .release_faults
        .store(1, Ordering::SeqCst);
    run(&gateway, Script::Reply(200, vec![chunk("a"), Step::Break]));
    one_hold(&gateway, HoldState::Released);
    assert_eq!(calls(&gateway), (0, 2));
    assert_eq!(gateway.services().threads.sleeps().len(), 1);
}

#[test]
fn lost_settle_acknowledgements_count_as_settled() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .settle_lost_acks
        .store(1, Ordering::SeqCst);
    run(
        &gateway,
        Script::Reply(200, vec![chunk("usage:5,1,20"), chunk("done")]),
    );
    one_hold(&gateway, HoldState::Settled(gpt_price(5, 1, 20)));
    assert_eq!(calls(&gateway), (2, 0));
    assert_eq!(gateway.services().threads.sleeps().len(), 1);
    assert_eq!(gateway.services().report.events(), []);
    gateway.services().wallet.check_books();
}

#[test]
fn lost_release_acknowledgements_count_as_released() {
    let gateway = stack();
    gateway
        .services()
        .wallet
        .release_lost_acks
        .store(1, Ordering::SeqCst);
    run(&gateway, Script::Reply(200, vec![chunk("a"), Step::Break]));
    one_hold(&gateway, HoldState::Released);
    assert_eq!(calls(&gateway), (0, 2));
    assert_eq!(gateway.services().report.events(), []);
    gateway.services().wallet.check_books();
}

fn ended_calls(gateway: &RelayGateway<Stack>) -> usize {
    gateway
        .services()
        .adapter
        .ended_calls
        .load(Ordering::SeqCst)
}

#[test]
fn the_meter_is_asked_once_after_the_body_is_read() {
    let gateway = stack();
    let mut steps = vec![chunk("a"); 40];
    steps.push(chunk("usage:5,1,20"));
    steps.push(chunk("done"));
    run(&gateway, Script::Reply(200, steps));
    assert_eq!(ended_calls(&gateway), 1);
    one_hold(&gateway, HoldState::Settled(gpt_price(5, 1, 20)));
}

#[test]
fn the_meter_is_not_asked_without_a_normal_end() {
    let gateway = stack();
    run(&gateway, Script::Reply(200, vec![chunk("a"), Step::Break]));
    assert_eq!(ended_calls(&gateway), 0);
    gateway
        .services()
        .upstream
        .push(Script::Reply(500, vec![chunk("a")]));
    let Ok(mut reply) = call(&gateway, None, "gpt") else {
        panic!("request failed");
    };
    drain(&mut reply.body);
    drop(reply);
    gateway.services().threads.join();
    assert_eq!(ended_calls(&gateway), 0);
}

#[test]
fn non_success_status_releases_and_forwards_the_body() {
    let gateway = stack();
    let (reply, chunks, interrupted) = run(
        &gateway,
        Script::Reply(400, vec![chunk("usage:5,1,20"), chunk("done")]),
    );
    assert_eq!(reply.status, 400);
    assert_eq!(chunks, ["usage:5,1,20", "done"]);
    assert!(!interrupted);
    released_without_charge(&gateway);
    let gateway = stack();
    let (reply, chunks, interrupted) =
        run(&gateway, Script::Reply(503, vec![chunk("x"), Step::Break]));
    assert_eq!(reply.status, 503);
    assert_eq!(chunks, ["x"]);
    assert!(interrupted);
    released_without_charge(&gateway);
    let gateway = stack();
    run(
        &gateway,
        Script::Reply(199, vec![chunk("usage:1,0,1"), chunk("done")]),
    );
    released_without_charge(&gateway);
    let gateway = stack();
    run(
        &gateway,
        Script::Reply(300, vec![chunk("usage:1,0,1"), chunk("done")]),
    );
    released_without_charge(&gateway);
    let gateway = stack();
    run(
        &gateway,
        Script::Reply(299, vec![chunk("usage:1,0,1"), chunk("done")]),
    );
    settled(&gateway, gpt_price(1, 0, 1));
}

#[test]
fn unreadable_ratio_follows_each_outcome() {
    let Ok(threshold) = UnreadableThreshold::new(2, 1, 2) else {
        panic!("threshold");
    };
    let gateway = stack_with(
        Some(KeyLimit {
            spend: None,
            requests_per_minute: None,
        }),
        GatewayConfig {
            unreadable_threshold: threshold,
            ..config(3)
        },
    );
    for unread in [true, true, true, false, false, false] {
        let body = if unread { "done" } else { "usage:1,0,1" };
        let mut steps = vec![chunk(body)];
        if !unread {
            steps.push(chunk("done"));
        }
        run(&gateway, Script::Reply(200, steps));
    }
    gateway.services().upstream.push(Script::Fail);
    let Err(GatewayError::Upstream) = call(&gateway, None, "gpt") else {
        panic!("upstream should fail");
    };
    gateway.services().threads.join();
    run(&gateway, Script::Reply(200, vec![chunk("done")]));
    let mut ratios = Vec::new();
    for event in gateway.services().report.events() {
        if let Event::Ratio(event) = event {
            ratios.push((event.decided, event.unreadable, event.window, event.at));
        }
    }
    assert_eq!(
        ratios,
        [(2, 2, 2, UnixMillis(NOW)), (8, 4, 2, UnixMillis(NOW))]
    );
    assert_eq!(gateway.services().wallet.revenue(), gpt_price(1, 0, 1) * 3);
    gateway.services().wallet.check_books();
}

#[test]
fn every_request_has_exactly_one_outcome() {
    let scripts = [
        Script::Reply(200, vec![chunk("usage:1,0,1"), chunk("done")]),
        Script::Reply(
            200,
            vec![chunk("usage:1,0,1"), chunk("usage:1,0,1"), chunk("done")],
        ),
        Script::Reply(200, vec![chunk("usage:1,0,1"), Step::Break]),
        Script::Reply(200, vec![chunk("a"), Step::Break]),
        Script::Reply(200, vec![chunk("garbled"), Step::Break]),
        Script::Reply(200, vec![chunk("garbled")]),
        Script::Reply(200, vec![chunk("done"), chunk("done")]),
        Script::Reply(500, vec![chunk("usage:1,0,1"), chunk("done")]),
        Script::Fail,
    ];
    for script in scripts {
        let gateway = stack();
        gateway.services().upstream.push(script);
        if let Ok(mut reply) = call(&gateway, None, "gpt") {
            drain(&mut reply.body);
        }
        gateway.services().threads.join();
        let (settles, releases) = calls(&gateway);
        let pending = usize::from(holds(&gateway)[0].1.state == HoldState::Open);
        assert_eq!(settles + releases + pending, 1);
        gateway.services().wallet.check_books();
    }
}
