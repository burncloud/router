use std::collections::VecDeque;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};

use router::ports::ByteStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    Head(u16),
    Failed,
    Chunk(Vec<u8>),
    Broken,
}

#[derive(Debug)]
struct Shared {
    queue: VecDeque<Message>,
    capacity: usize,
    sender_alive: bool,
    receiver_alive: bool,
    receiver_waker: Option<Waker>,
    sender_waker: Option<Waker>,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    match shared.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[derive(Debug)]
pub(crate) struct Sender {
    shared: Arc<Mutex<Shared>>,
}

#[derive(Debug)]
pub(crate) struct Receiver {
    shared: Arc<Mutex<Shared>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Closed;

pub(crate) fn channel(capacity: NonZeroUsize) -> (Sender, Receiver) {
    let shared = Arc::new(Mutex::new(Shared {
        queue: VecDeque::new(),
        capacity: capacity.get(),
        sender_alive: true,
        receiver_alive: true,
        receiver_waker: None,
        sender_waker: None,
    }));
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

impl Sender {
    pub(crate) async fn send(&mut self, message: Message) -> Result<(), Closed> {
        let mut slot = Some(message);
        poll_fn(|context| {
            let mut shared = lock(&self.shared);
            if !shared.receiver_alive {
                return Poll::Ready(Err(Closed));
            }
            if shared.queue.len() < shared.capacity {
                if let Some(message) = slot.take() {
                    shared.queue.push_back(message);
                }
                let waker = shared.receiver_waker.take();
                drop(shared);
                wake(waker);
                return Poll::Ready(Ok(()));
            }
            shared.sender_waker = Some(context.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut shared = lock(&self.shared);
        shared.sender_alive = false;
        let waker = shared.receiver_waker.take();
        drop(shared);
        wake(waker);
    }
}

impl Receiver {
    pub(crate) async fn recv(&mut self) -> Option<Message> {
        poll_fn(|context| {
            let mut shared = lock(&self.shared);
            if let Some(message) = shared.queue.pop_front() {
                let waker = shared.sender_waker.take();
                drop(shared);
                wake(waker);
                return Poll::Ready(Some(message));
            }
            if !shared.sender_alive {
                return Poll::Ready(None);
            }
            shared.receiver_waker = Some(context.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let mut shared = lock(&self.shared);
        shared.receiver_alive = false;
        shared.queue.clear();
        let waker = shared.sender_waker.take();
        drop(shared);
        wake(waker);
    }
}

/// 上游在转发途中断开。客户端应直接关闭连接，流式不补 `[DONE]`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("upstream body interrupted")]
pub struct BodyInterrupted;

/// 丢弃它表示客户端断开，后台任务停止转发，但仍读完上游并结算。
#[derive(Debug)]
pub struct ReplyBody {
    receiver: Receiver,
}

impl ReplyBody {
    pub(crate) fn new(receiver: Receiver) -> Self {
        Self { receiver }
    }
}

impl ByteStream for ReplyBody {
    type Error = BodyInterrupted;

    async fn next_chunk(&mut self) -> Option<Result<Vec<u8>, BodyInterrupted>> {
        match self.receiver.recv().await? {
            Message::Chunk(bytes) => Some(Ok(bytes)),
            Message::Head(_) | Message::Failed | Message::Broken => Some(Err(BodyInterrupted)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::{channel, Closed, Message};

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        let mut future = pin!(future);
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    fn bytes(byte: u8) -> Message {
        Message::Chunk(vec![byte])
    }

    #[test]
    fn capacity_bounds_the_queue() {
        let (mut sender, mut receiver) = channel(NonZeroUsize::MIN);
        assert_eq!(poll_once(sender.send(bytes(1))), Poll::Ready(Ok(())));
        assert_eq!(poll_once(sender.send(bytes(2))), Poll::Pending);
        assert_eq!(poll_once(receiver.recv()), Poll::Ready(Some(bytes(1))));
        assert_eq!(poll_once(receiver.recv()), Poll::Pending);
        assert_eq!(poll_once(sender.send(bytes(3))), Poll::Ready(Ok(())));
        drop(sender);
        assert_eq!(poll_once(receiver.recv()), Poll::Ready(Some(bytes(3))));
        assert_eq!(poll_once(receiver.recv()), Poll::Ready(None));
    }

    #[test]
    fn dropped_receiver_closes_the_sender() {
        let (mut sender, receiver) = channel(NonZeroUsize::MIN);
        drop(receiver);
        assert_eq!(poll_once(sender.send(bytes(1))), Poll::Ready(Err(Closed)));
    }
}
