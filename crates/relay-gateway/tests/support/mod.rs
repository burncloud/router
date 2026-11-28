#![allow(
    dead_code,
    reason = "each test binary uses a different part of the support code"
)]

use std::collections::{HashMap, VecDeque};
use std::future::{ready, Future};
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::Duration;

use billing::errors::{BillingRejection, WalletError};
use billing::model::{Direction, Entry, KeyCap, LedgerAccount, ModelUsage, TokenUsage};
use billing::ports::{Pricing, Wallet};
use identity::model::{Admission, KeyLimit, Principal, RequestsPerMinute};
use identity::ports::{ApiKeys, RateLimiter};
use kernel::model::{AccountId, Currency, KeyId, ModelId, Money, RequestId, UnixMillis};
use kernel::ports::Clock;
use relay_gateway::{
    Fault, GatewayConfig, PriceSource, RelayGateway, Report, RequestIds, Services, Shortfall, Spawn,
};
use router::model::{Prepared, UpstreamReply};
use router::ports::{Adapter, ByteStream, Upstream, UsageMeter};
use runtime::events::{SettlementCapped, UsageUnreadable};

pub const KEY: &str = "sk-test";
pub const ACCOUNT: &str = "acct-1";
pub const KEY_ID: &str = "key-1";
pub const NOW: u64 = 1_795_000_000_000;

pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct Signal {
    ready: Mutex<bool>,
    condvar: Condvar,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        *lock(&self.ready) = true;
        self.condvar.notify_one();
    }
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let signal = Arc::new(Signal {
        ready: Mutex::new(false),
        condvar: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
        let mut ready = lock(&signal.ready);
        while !*ready {
            ready = match signal.condvar.wait(ready) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        *ready = false;
    }
}

#[derive(Debug, Default)]
pub struct Threads {
    handles: Mutex<Vec<JoinHandle<()>>>,
    sleeps: Mutex<Vec<Duration>>,
}

impl Threads {
    pub fn join(&self) {
        loop {
            let handle = lock(&self.handles).pop();
            let Some(handle) = handle else {
                return;
            };
            assert!(handle.join().is_ok());
        }
    }

    pub fn sleeps(&self) -> Vec<Duration> {
        lock(&self.sleeps).clone()
    }
}

impl Spawn for Threads {
    fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = std::thread::spawn(move || block_on(task));
        lock(&self.handles).push(handle);
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        lock(&self.sleeps).push(duration);
        ready(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Unreadable(UsageUnreadable),
    Capped(SettlementCapped, Shortfall),
    Fault(Fault),
}

#[derive(Debug, Default)]
pub struct Recorder {
    events: Mutex<Vec<Event>>,
}

impl Recorder {
    pub fn events(&self) -> Vec<Event> {
        lock(&self.events).clone()
    }
}

impl Report for Recorder {
    fn usage_unreadable(&self, event: &UsageUnreadable) {
        lock(&self.events).push(Event::Unreadable(event.clone()));
    }

    fn settlement_capped(&self, event: &SettlementCapped, shortfall: Shortfall) {
        lock(&self.events).push(Event::Capped(event.clone(), shortfall));
    }

    fn fault(&self, fault: &Fault) {
        lock(&self.events).push(Event::Fault(fault.clone()));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreDown;

#[derive(Debug, Default)]
pub struct Keys {
    pub entries: Mutex<HashMap<String, (Principal, Option<KeyLimit>)>>,
    pub broken: AtomicBool,
    pub calls: AtomicUsize,
}

impl Keys {
    pub fn with_limit(limit: Option<KeyLimit>) -> Self {
        let keys = Self::default();
        lock(&keys.entries).insert(
            KEY.to_owned(),
            (
                Principal {
                    account: AccountId(ACCOUNT.to_owned()),
                    key: KeyId(KEY_ID.to_owned()),
                },
                limit,
            ),
        );
        keys
    }
}

impl ApiKeys for Keys {
    type Error = StoreDown;

    fn authenticate(
        &self,
        key: &str,
    ) -> impl Future<Output = Result<Option<Principal>, StoreDown>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = if self.broken.load(Ordering::SeqCst) {
            Err(StoreDown)
        } else {
            Ok(lock(&self.entries)
                .get(key)
                .map(|(principal, _)| principal.clone()))
        };
        ready(result)
    }

    fn limit(
        &self,
        key: &KeyId,
    ) -> impl Future<Output = Result<Option<KeyLimit>, StoreDown>> + Send {
        let found = lock(&self.entries)
            .values()
            .find(|(principal, _)| &principal.key == key)
            .and_then(|(_, limit)| *limit);
        ready(Ok(found))
    }
}

#[derive(Debug, Default)]
pub struct Limiter {
    pub throttle: AtomicBool,
    pub calls: Mutex<Vec<(KeyId, RequestsPerMinute, UnixMillis)>>,
}

impl RateLimiter for Limiter {
    type Error = StoreDown;

    fn admit(
        &self,
        key: &KeyId,
        limit: RequestsPerMinute,
        now: UnixMillis,
    ) -> impl Future<Output = Result<Admission, StoreDown>> + Send {
        lock(&self.calls).push((key.clone(), limit, now));
        let admission = if self.throttle.load(Ordering::SeqCst) {
            Admission::Throttled
        } else {
            Admission::Allowed
        };
        ready(Ok(admission))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsupported;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeterFault {
    Garbled,
    Duplicate,
    Missing,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ScriptMeter {
    usage: Option<TokenUsage>,
    done: bool,
    failure: Option<MeterFault>,
}

fn parse_usage(text: &str) -> Option<TokenUsage> {
    let mut parts = text.split(',').map(str::parse::<u64>);
    let (Some(Ok(input)), Some(Ok(cached)), Some(Ok(output)), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    Some(TokenUsage {
        input_tokens: input,
        cached_input_tokens: cached,
        output_tokens: output,
    })
}

impl UsageMeter for ScriptMeter {
    type Error = MeterFault;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), MeterFault> {
        let text = String::from_utf8_lossy(bytes);
        if text == "done" {
            self.done = true;
        } else if text == "garbled" {
            self.failure.get_or_insert(MeterFault::Garbled);
        } else if let Some(usage) = text.strip_prefix("usage:") {
            match (self.usage, parse_usage(usage)) {
                (None, Some(parsed)) => self.usage = Some(parsed),
                (Some(_), _) => {
                    self.failure.get_or_insert(MeterFault::Duplicate);
                }
                (None, None) => {
                    self.failure.get_or_insert(MeterFault::Garbled);
                }
            }
        }
        self.failure.map_or(Ok(()), Err)
    }

    fn ended(&self) -> bool {
        self.done
    }

    fn finish(self) -> Result<TokenUsage, MeterFault> {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        self.usage.ok_or(MeterFault::Missing)
    }
}

pub const MAX_OUTPUT: u64 = 1_000;

#[derive(Debug, Default)]
pub struct ScriptAdapter {
    pub streaming: AtomicBool,
}

impl Adapter for ScriptAdapter {
    type Error = Unsupported;
    type Meter = ScriptMeter;

    fn prepare(&self, body: &[u8]) -> Result<Prepared<ScriptMeter>, Unsupported> {
        let model = String::from_utf8_lossy(body).into_owned();
        if model == "reject" {
            return Err(Unsupported);
        }
        let mut upstream_body = b"up:".to_vec();
        upstream_body.extend_from_slice(body);
        Ok(Prepared {
            limit: ModelUsage {
                model: ModelId(model),
                tokens: TokenUsage {
                    input_tokens: body.len() as u64,
                    cached_input_tokens: 0,
                    output_tokens: MAX_OUTPUT,
                },
            },
            upstream_body,
            streaming: self.streaming.load(Ordering::SeqCst),
            meter: ScriptMeter::default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Chunk(Vec<u8>),
    Break,
}

pub fn chunk(text: &str) -> Step {
    Step::Chunk(text.as_bytes().to_vec())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Script {
    Fail,
    Reply(u16, Vec<Step>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unreachable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reset;

#[derive(Debug)]
pub struct ScriptBody {
    steps: VecDeque<Step>,
    read: Arc<AtomicUsize>,
    exhausted: Arc<AtomicBool>,
}

impl ByteStream for ScriptBody {
    type Error = Reset;

    fn next_chunk(&mut self) -> impl Future<Output = Option<Result<Vec<u8>, Reset>>> + Send {
        let next = match self.steps.pop_front() {
            Some(Step::Chunk(bytes)) => {
                self.read.fetch_add(1, Ordering::SeqCst);
                Some(Ok(bytes))
            }
            Some(Step::Break) => {
                self.exhausted.store(true, Ordering::SeqCst);
                Some(Err(Reset))
            }
            None => {
                self.exhausted.store(true, Ordering::SeqCst);
                None
            }
        };
        ready(next)
    }
}

#[derive(Debug, Default)]
pub struct ScriptUpstream {
    pub scripts: Mutex<VecDeque<Script>>,
    pub bodies: Mutex<Vec<(Vec<u8>, bool)>>,
    pub read: Arc<AtomicUsize>,
    pub exhausted: Arc<AtomicBool>,
}

impl ScriptUpstream {
    pub fn push(&self, script: Script) {
        lock(&self.scripts).push_back(script);
    }

    pub fn calls(&self) -> usize {
        lock(&self.bodies).len()
    }
}

impl Upstream for ScriptUpstream {
    type Error = Unreachable;
    type Body = ScriptBody;

    fn call(
        &self,
        body: Vec<u8>,
        streaming: bool,
    ) -> impl Future<Output = Result<UpstreamReply<ScriptBody>, Unreachable>> + Send {
        lock(&self.bodies).push((body, streaming));
        let script = lock(&self.scripts).pop_front();
        let result = match script {
            Some(Script::Reply(status, steps)) => Ok(UpstreamReply {
                status,
                body: ScriptBody {
                    steps: steps.into(),
                    read: Arc::clone(&self.read),
                    exhausted: Arc::clone(&self.exhausted),
                },
            }),
            Some(Script::Fail) | None => Err(Unreachable),
        };
        ready(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rate {
    pub currency: Currency,
    pub input: u64,
    pub cached: u64,
    pub output: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceTable {
    rates: HashMap<ModelId, Rate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceError {
    UnknownModel,
    Overflow,
}

impl Pricing for PriceTable {
    type Usage = ModelUsage;
    type Error = PriceError;

    fn price(&self, usage: &ModelUsage) -> Result<Money, PriceError> {
        let rate = self
            .rates
            .get(&usage.model)
            .ok_or(PriceError::UnknownModel)?;
        let tokens = usage.tokens;
        let minor_units = tokens
            .input_tokens
            .checked_mul(rate.input)
            .zip(tokens.cached_input_tokens.checked_mul(rate.cached))
            .zip(tokens.output_tokens.checked_mul(rate.output))
            .and_then(|((input, cached), output)| input.checked_add(cached)?.checked_add(output))
            .ok_or(PriceError::Overflow)?;
        Ok(Money {
            currency: rate.currency,
            minor_units,
        })
    }
}

pub const GPT: Rate = Rate {
    currency: Currency::Usd,
    input: 2,
    cached: 1,
    output: 10,
};

pub fn gpt_price(input: u64, cached: u64, output: u64) -> u64 {
    input * GPT.input + cached * GPT.cached + output * GPT.output
}

#[derive(Debug)]
pub struct Prices {
    pub table: PriceTable,
    pub broken: AtomicBool,
    pub loads: AtomicUsize,
}

impl Default for Prices {
    fn default() -> Self {
        let rates = [
            ("gpt", GPT),
            (
                "huge",
                Rate {
                    currency: Currency::Usd,
                    input: 1,
                    cached: 1,
                    output: u64::MAX / 2_000,
                },
            ),
            (
                "free",
                Rate {
                    currency: Currency::Usd,
                    input: 0,
                    cached: 0,
                    output: 0,
                },
            ),
            (
                "yuan",
                Rate {
                    currency: Currency::Cny,
                    input: 1,
                    cached: 1,
                    output: 1,
                },
            ),
        ]
        .into_iter()
        .map(|(model, rate)| (ModelId(model.to_owned()), rate))
        .collect();
        Self {
            table: PriceTable { rates },
            broken: AtomicBool::new(false),
            loads: AtomicUsize::new(0),
        }
    }
}

impl PriceSource for Prices {
    type Prices = PriceTable;
    type Error = StoreDown;

    fn snapshot(&self) -> impl Future<Output = Result<PriceTable, StoreDown>> + Send {
        self.loads.fetch_add(1, Ordering::SeqCst);
        let result = if self.broken.load(Ordering::SeqCst) {
            Err(StoreDown)
        } else {
            Ok(self.table.clone())
        };
        ready(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldState {
    Open,
    Settled(u64),
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hold {
    pub amount: Money,
    pub state: HoldState,
}

#[derive(Debug, Default)]
struct Books {
    available: HashMap<AccountId, u64>,
    deposits: u64,
    holds: HashMap<(AccountId, RequestId), (KeyId, Hold)>,
    entries: Vec<Entry>,
}

#[derive(Debug, Default)]
pub struct LedgerWallet {
    books: Mutex<Books>,
    pub settle_faults: AtomicU32,
    pub release_faults: AtomicU32,
    pub hold_fault: AtomicBool,
    pub settle_calls: AtomicUsize,
    pub release_calls: AtomicUsize,
    pub caps: Mutex<Vec<Option<KeyCap>>>,
    pub clock: AtomicU64,
}

fn usd(minor_units: u64) -> Money {
    Money {
        currency: Currency::Usd,
        minor_units,
    }
}

fn take_fault(counter: &AtomicU32) -> bool {
    counter
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

impl LedgerWallet {
    pub fn deposit(&self, account: &str, minor_units: u64) {
        let mut books = lock(&self.books);
        *books
            .available
            .entry(AccountId(account.to_owned()))
            .or_insert(0) += minor_units;
        books.deposits += minor_units;
    }

    pub fn available(&self, account: &str) -> u64 {
        lock(&self.books)
            .available
            .get(&AccountId(account.to_owned()))
            .copied()
            .unwrap_or(0)
    }

    pub fn holds(&self) -> Vec<(RequestId, Hold)> {
        let mut holds: Vec<_> = lock(&self.books)
            .holds
            .iter()
            .map(|((_, request), (_, hold))| (request.clone(), *hold))
            .collect();
        holds.sort_by(|left, right| left.0 .0.cmp(&right.0 .0));
        holds
    }

    pub fn entries(&self) -> Vec<Entry> {
        lock(&self.books).entries.clone()
    }

    pub fn revenue(&self) -> u64 {
        sum(&self.entries(), &LedgerAccount::Revenue, Direction::Credit)
    }

    pub fn check_books(&self) {
        let books = lock(&self.books);
        let debits: u64 = books
            .entries
            .iter()
            .filter(|entry| entry.direction == Direction::Debit)
            .map(|entry| entry.amount.minor_units)
            .sum();
        let credits: u64 = books
            .entries
            .iter()
            .filter(|entry| entry.direction == Direction::Credit)
            .map(|entry| entry.amount.minor_units)
            .sum();
        assert_eq!(debits, credits);
        let available: u64 = books.available.values().sum();
        let held: u64 = books
            .holds
            .values()
            .filter(|(_, hold)| hold.state == HoldState::Open)
            .map(|(_, hold)| hold.amount.minor_units)
            .sum();
        let revenue = sum(&books.entries, &LedgerAccount::Revenue, Direction::Credit);
        assert_eq!(available + held + revenue, books.deposits);
    }

    fn entry(
        books: &mut Books,
        request: &RequestId,
        account: LedgerAccount,
        key: &KeyId,
        direction: Direction,
        amount: u64,
        at: u64,
    ) {
        books.entries.push(Entry {
            request: request.clone(),
            account,
            key: Some(key.clone()),
            direction,
            amount: usd(amount),
            at: UnixMillis(at),
        });
    }
}

fn sum(entries: &[Entry], account: &LedgerAccount, direction: Direction) -> u64 {
    entries
        .iter()
        .filter(|entry| &entry.account == account && entry.direction == direction)
        .map(|entry| entry.amount.minor_units)
        .sum()
}

impl Wallet for LedgerWallet {
    type Error = StoreDown;

    fn balance(
        &self,
        account: &AccountId,
        _currency: Currency,
    ) -> impl Future<Output = Result<Money, WalletError<StoreDown>>> + Send {
        let available = lock(&self.books).available.get(account).copied();
        ready(available.map(usd).ok_or(WalletError::Storage(StoreDown)))
    }

    fn hold(
        &self,
        account: &AccountId,
        request: &RequestId,
        key: &KeyId,
        cap: Option<KeyCap>,
        amount: Money,
    ) -> impl Future<Output = Result<(), WalletError<StoreDown>>> + Send {
        lock(&self.caps).push(cap);
        let at = self.clock.load(Ordering::SeqCst);
        let result = (|| {
            if self.hold_fault.load(Ordering::SeqCst) {
                return Err(WalletError::Storage(StoreDown));
            }
            let mut books = lock(&self.books);
            let hold_key = (account.clone(), request.clone());
            if books.holds.contains_key(&hold_key) {
                return Err(WalletError::Rejected(BillingRejection::DuplicateRequest));
            }
            if amount.currency != Currency::Usd {
                return Err(WalletError::Rejected(BillingRejection::CurrencyMismatch));
            }
            if let Some(cap) = cap {
                if cap.limit.currency != amount.currency {
                    return Err(WalletError::Rejected(BillingRejection::CurrencyMismatch));
                }
                let spent: u64 = books
                    .holds
                    .values()
                    .filter(|(holder, _)| holder == key)
                    .map(|(_, hold)| match hold.state {
                        HoldState::Open => hold.amount.minor_units,
                        HoldState::Settled(actual) => actual,
                        HoldState::Released => 0,
                    })
                    .sum();
                if spent + amount.minor_units > cap.limit.minor_units {
                    return Err(WalletError::Rejected(BillingRejection::LimitExceeded));
                }
            }
            let available = books.available.get(account).copied().unwrap_or(0);
            if available < amount.minor_units {
                return Err(WalletError::Rejected(BillingRejection::InsufficientBalance));
            }
            books
                .available
                .insert(account.clone(), available - amount.minor_units);
            books.holds.insert(
                hold_key,
                (
                    key.clone(),
                    Hold {
                        amount,
                        state: HoldState::Open,
                    },
                ),
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Available(account.clone()),
                key,
                Direction::Debit,
                amount.minor_units,
                at,
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Held(account.clone()),
                key,
                Direction::Credit,
                amount.minor_units,
                at,
            );
            Ok(())
        })();
        ready(result)
    }

    fn settle(
        &self,
        account: &AccountId,
        request: &RequestId,
        amount: Money,
    ) -> impl Future<Output = Result<(), WalletError<StoreDown>>> + Send {
        self.settle_calls.fetch_add(1, Ordering::SeqCst);
        let at = self.clock.load(Ordering::SeqCst);
        let result = (|| {
            if take_fault(&self.settle_faults) {
                return Err(WalletError::Storage(StoreDown));
            }
            let mut books = lock(&self.books);
            let Some((key, hold)) = books
                .holds
                .get(&(account.clone(), request.clone()))
                .cloned()
            else {
                return Err(WalletError::Rejected(BillingRejection::HoldNotFound));
            };
            match hold.state {
                HoldState::Open => {}
                HoldState::Settled(_) => {
                    return Err(WalletError::Rejected(BillingRejection::AlreadySettled))
                }
                HoldState::Released => {
                    return Err(WalletError::Rejected(BillingRejection::AlreadyReleased))
                }
            }
            if amount.minor_units > hold.amount.minor_units {
                return Err(WalletError::Rejected(
                    BillingRejection::SettlementExceedsHold,
                ));
            }
            let refund = hold.amount.minor_units - amount.minor_units;
            *books.available.entry(account.clone()).or_insert(0) += refund;
            books.holds.insert(
                (account.clone(), request.clone()),
                (
                    key.clone(),
                    Hold {
                        amount: hold.amount,
                        state: HoldState::Settled(amount.minor_units),
                    },
                ),
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Held(account.clone()),
                &key,
                Direction::Debit,
                hold.amount.minor_units,
                at,
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Revenue,
                &key,
                Direction::Credit,
                amount.minor_units,
                at,
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Available(account.clone()),
                &key,
                Direction::Credit,
                refund,
                at,
            );
            Ok(())
        })();
        ready(result)
    }

    fn release(
        &self,
        account: &AccountId,
        request: &RequestId,
    ) -> impl Future<Output = Result<(), WalletError<StoreDown>>> + Send {
        self.release_calls.fetch_add(1, Ordering::SeqCst);
        let at = self.clock.load(Ordering::SeqCst);
        let result = (|| {
            if take_fault(&self.release_faults) {
                return Err(WalletError::Storage(StoreDown));
            }
            let mut books = lock(&self.books);
            let Some((key, hold)) = books
                .holds
                .get(&(account.clone(), request.clone()))
                .cloned()
            else {
                return Err(WalletError::Rejected(BillingRejection::HoldNotFound));
            };
            match hold.state {
                HoldState::Open => {}
                HoldState::Settled(_) => {
                    return Err(WalletError::Rejected(BillingRejection::AlreadySettled))
                }
                HoldState::Released => {
                    return Err(WalletError::Rejected(BillingRejection::AlreadyReleased))
                }
            }
            *books.available.entry(account.clone()).or_insert(0) += hold.amount.minor_units;
            books.holds.insert(
                (account.clone(), request.clone()),
                (
                    key.clone(),
                    Hold {
                        amount: hold.amount,
                        state: HoldState::Released,
                    },
                ),
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Held(account.clone()),
                &key,
                Direction::Debit,
                hold.amount.minor_units,
                at,
            );
            Self::entry(
                &mut books,
                request,
                LedgerAccount::Available(account.clone()),
                &key,
                Direction::Credit,
                hold.amount.minor_units,
                at,
            );
            Ok(())
        })();
        ready(result)
    }

    fn held(
        &self,
        account: &AccountId,
        request: &RequestId,
    ) -> impl Future<Output = Result<Money, WalletError<StoreDown>>> + Send {
        let result = lock(&self.books)
            .holds
            .get(&(account.clone(), request.clone()))
            .map(|(_, hold)| match hold.state {
                HoldState::Open => hold.amount,
                HoldState::Settled(_) | HoldState::Released => usd(0),
            })
            .ok_or(WalletError::Rejected(BillingRejection::HoldNotFound));
        ready(result)
    }

    fn spent(
        &self,
        key: &KeyId,
        _currency: Currency,
        _since: UnixMillis,
    ) -> impl Future<Output = Result<Money, WalletError<StoreDown>>> + Send {
        let spent = lock(&self.books)
            .holds
            .values()
            .filter(|(holder, _)| holder == key)
            .map(|(_, hold)| match hold.state {
                HoldState::Open => hold.amount.minor_units,
                HoldState::Settled(actual) => actual,
                HoldState::Released => 0,
            })
            .sum();
        ready(Ok(usd(spent)))
    }
}

#[derive(Debug)]
pub struct FixedClock(pub AtomicU64);

impl Clock for FixedClock {
    fn now(&self) -> UnixMillis {
        UnixMillis(self.0.load(Ordering::SeqCst))
    }
}

#[derive(Debug, Default)]
pub struct Counter(AtomicUsize);

impl RequestIds for Counter {
    fn next_id(&self) -> RequestId {
        let next = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        RequestId(format!("gen-{next}"))
    }
}

#[derive(Debug)]
pub struct Stack {
    pub keys: Keys,
    pub limiter: Limiter,
    pub adapter: ScriptAdapter,
    pub upstream: ScriptUpstream,
    pub prices: Prices,
    pub wallet: LedgerWallet,
    pub clock: FixedClock,
    pub threads: Threads,
    pub ids: Counter,
    pub report: Recorder,
    pub config: GatewayConfig,
}

impl Services for Stack {
    type Keys = Keys;
    type Limiter = Limiter;
    type Adapter = ScriptAdapter;
    type Upstream = ScriptUpstream;
    type Prices = Prices;
    type Wallet = LedgerWallet;
    type Clock = FixedClock;
    type Spawn = Threads;
    type Ids = Counter;
    type Report = Recorder;

    fn keys(&self) -> &Keys {
        &self.keys
    }
    fn limiter(&self) -> &Limiter {
        &self.limiter
    }
    fn adapter(&self) -> &ScriptAdapter {
        &self.adapter
    }
    fn upstream(&self) -> &ScriptUpstream {
        &self.upstream
    }
    fn prices(&self) -> &Prices {
        &self.prices
    }
    fn wallet(&self) -> &LedgerWallet {
        &self.wallet
    }
    fn clock(&self) -> &FixedClock {
        &self.clock
    }
    fn spawner(&self) -> &Threads {
        &self.threads
    }
    fn ids(&self) -> &Counter {
        &self.ids
    }
    fn report(&self) -> &Recorder {
        &self.report
    }
    fn config(&self) -> &GatewayConfig {
        &self.config
    }
}

pub const BALANCE: u64 = 1_000_000;

pub fn config(attempts: u32) -> GatewayConfig {
    GatewayConfig {
        channel_capacity: NonZeroUsize::MIN,
        settle_attempts: NonZeroU32::new(attempts).unwrap_or(NonZeroU32::MIN),
        retry_delay: Duration::from_millis(7),
    }
}

pub fn stack_with(limit: Option<KeyLimit>, attempts: u32) -> RelayGateway<Stack> {
    let wallet = LedgerWallet::default();
    wallet.deposit(ACCOUNT, BALANCE);
    wallet.clock.store(NOW, Ordering::SeqCst);
    RelayGateway::new(Stack {
        keys: Keys::with_limit(limit),
        limiter: Limiter::default(),
        adapter: ScriptAdapter::default(),
        upstream: ScriptUpstream::default(),
        prices: Prices::default(),
        wallet,
        clock: FixedClock(AtomicU64::new(NOW)),
        threads: Threads::default(),
        ids: Counter::default(),
        report: Recorder::default(),
        config: config(attempts),
    })
}

pub fn stack() -> RelayGateway<Stack> {
    stack_with(
        Some(KeyLimit {
            spend: None,
            requests_per_minute: None,
        }),
        3,
    )
}
