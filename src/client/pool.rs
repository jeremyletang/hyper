use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use futures::{Future, Async, Poll};
use futures::sync::oneshot;

pub struct Pool<T> {
    inner: Arc<Mutex<PoolInner<T>>>,
}

// Before using a pooled connection, make sure the sender is not dead.
//
// This is a trait to allow the `client::pool::tests` to work for `i32`.
//
// See https://github.com/hyperium/hyper/issues/1429
pub trait Ready {
    fn poll_ready(&mut self) -> Poll<(), ()>;
}

struct PoolInner<T> {
    enabled: bool,
    // These are internal Conns sitting in the event loop in the KeepAlive
    // state, waiting to receive a new Request to send on the socket.
    idle: HashMap<Arc<String>, Vec<Idle<T>>>,
    // These are outstanding Checkouts that are waiting for a socket to be
    // able to send a Request one. This is used when "racing" for a new
    // connection.
    //
    // The Client starts 2 tasks, 1 to connect a new socket, and 1 to wait
    // for the Pool to receive an idle Conn. When a Conn becomes idle,
    // this list is checked for any parked Checkouts, and tries to notify
    // them that the Conn could be used instead of waiting for a brand new
    // connection.
    parked: HashMap<Arc<String>, VecDeque<oneshot::Sender<T>>>,
    timeout: Option<Duration>,
}

impl<T> Pool<T> {
    pub fn new(enabled: bool, timeout: Option<Duration>) -> Pool<T> {
        Pool {
            inner: Arc::new(Mutex::new(PoolInner {
                enabled: enabled,
                idle: HashMap::new(),
                parked: HashMap::new(),
                timeout: timeout,
            })),
        }
    }
}

impl<T: Ready> Pool<T> {
    pub fn checkout(&self, key: &str) -> Checkout<T> {
        Checkout {
            key: Arc::new(key.to_owned()),
            pool: self.clone(),
            parked: None,
        }
    }

    fn put(&mut self, key: Arc<String>, value: T) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.enabled {
            return;
        }
        trace!("Pool::put {:?}", key);
        let mut remove_parked = false;
        let mut value = Some(value);
        if let Some(parked) = inner.parked.get_mut(&key) {
            while let Some(tx) = parked.pop_front() {
                if !tx.is_canceled() {
                    match tx.send(value.take().unwrap()) {
                        Ok(()) => break,
                        Err(e) => {
                            value = Some(e);
                        }
                    }
                }

                trace!("Pool::put removing canceled parked {:?}", key);
            }
            remove_parked = parked.is_empty();
        }
        if remove_parked {
            inner.parked.remove(&key);
        }

        match value {
            Some(value) => {
                debug!("pooling idle connection for {:?}", key);
                inner.idle.entry(key)
                     .or_insert(Vec::new())
                     .push(Idle {
                         value: value,
                         idle_at: Instant::now(),
                     });
            }
            None => trace!("Pool::put found parked {:?}", key),
        }
    }

    fn take(&self, key: &Arc<String>) -> Option<Pooled<T>> {
        let entry = {
            let mut inner = self.inner.lock().unwrap();
            let expiration = Expiration::new(inner.timeout);
            let mut should_remove = false;
            let entry = inner.idle.get_mut(key).and_then(|list| {
                trace!("take; url = {:?}, expiration = {:?}", key, expiration.0);
                while let Some(mut entry) = list.pop() {
                    if !expiration.expires(entry.idle_at) {
                        if let Ok(Async::Ready(())) = entry.value.poll_ready() {
                            should_remove = list.is_empty();
                            return Some(entry);
                        }
                    }
                    trace!("removing unacceptable pooled {:?}", key);
                    // every other case the Idle should just be dropped
                    // 1. Idle but expired
                    // 2. Busy (something else somehow took it?)
                    // 3. Disabled don't reuse of course
                }
                should_remove = true;
                None
            });

            if should_remove {
                inner.idle.remove(key);
            }
            entry
        };

        entry.map(|e| self.reuse(key, e.value))
    }


    pub fn pooled(&self, key: Arc<String>, value: T) -> Pooled<T> {
        Pooled {
            is_reused: false,
            key: key,
            pool: Arc::downgrade(&self.inner),
            value: Some(value)
        }
    }

    fn reuse(&self, key: &Arc<String>, value: T) -> Pooled<T> {
        debug!("reuse idle connection for {:?}", key);
        Pooled {
            is_reused: true,
            key: key.clone(),
            pool: Arc::downgrade(&self.inner),
            value: Some(value),
        }
    }

    fn park(&mut self, key: Arc<String>, tx: oneshot::Sender<T>) {
        trace!("park; waiting for idle connection: {:?}", key);
        self.inner.lock().unwrap()
            .parked.entry(key)
            .or_insert(VecDeque::new())
            .push_back(tx);
    }
}

impl<T> Pool<T> {
    /// Any `FutureResponse`s that were created will have made a `Checkout`,
    /// and possibly inserted into the pool that it is waiting for an idle
    /// connection. If a user ever dropped that future, we need to clean out
    /// those parked senders.
    fn clean_parked(&mut self, key: &Arc<String>) {
        let mut inner = self.inner.lock().unwrap();

        let mut remove_parked = false;
        if let Some(parked) = inner.parked.get_mut(key) {
            parked.retain(|tx| {
                !tx.is_canceled()
            });
            remove_parked = parked.is_empty();
        }
        if remove_parked {
            inner.parked.remove(key);
        }
    }
}

impl<T> Clone for Pool<T> {
    fn clone(&self) -> Pool<T> {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

pub struct Pooled<T: Ready> {
    value: Option<T>,
    is_reused: bool,
    key: Arc<String>,
    pool: Weak<Mutex<PoolInner<T>>>,
}

impl<T: Ready> Pooled<T> {
    pub fn is_reused(&self) -> bool {
        self.is_reused
    }

    fn as_ref(&self) -> &T {
        self.value.as_ref().expect("not dropped")
    }

    fn as_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("not dropped")
    }
}

impl<T: Ready> Deref for Pooled<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T: Ready> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.as_mut()
    }
}

impl<T: Ready> Drop for Pooled<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if let Some(inner) = self.pool.upgrade() {
                let mut pool = Pool {
                    inner: inner,
                };

                pool.put(self.key.clone(), value);
            } else {
                trace!("pool dropped, dropping pooled ({:?})", self.key);
            }
        }
    }
}

impl<T: Ready> fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Pooled")
            .field("key", &self.key)
            .finish()
    }
}

struct Idle<T> {
    idle_at: Instant,
    value: T,
}

pub struct Checkout<T> {
    key: Arc<String>,
    pool: Pool<T>,
    parked: Option<oneshot::Receiver<T>>,
}

struct NotParked;

impl<T: Ready> Checkout<T> {
    fn poll_parked(&mut self) -> Poll<Pooled<T>, NotParked> {
        let mut drop_parked = false;
        if let Some(ref mut rx) = self.parked {
            match rx.poll() {
                Ok(Async::Ready(mut value)) => {
                    if let Ok(Async::Ready(())) = value.poll_ready() {
                        return Ok(Async::Ready(self.pool.reuse(&self.key, value)));
                    }
                    drop_parked = true;
                },
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(_canceled) => drop_parked = true,
            }
        }
        if drop_parked {
            self.parked.take();
        }
        Err(NotParked)
    }

    fn park(&mut self) {
        if self.parked.is_none() {
            let (tx, mut rx) = oneshot::channel();
            let _ = rx.poll(); // park this task
            self.pool.park(self.key.clone(), tx);
            self.parked = Some(rx);
        }
    }
}

impl<T: Ready> Future for Checkout<T> {
    type Item = Pooled<T>;
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.poll_parked() {
            Ok(async) => return Ok(async),
            Err(_not_parked) => (),
        }

        let entry = self.pool.take(&self.key);

        if let Some(pooled) = entry {
            Ok(Async::Ready(pooled))
        } else {
            self.park();
            Ok(Async::NotReady)
        }
    }
}

impl<T> Drop for Checkout<T> {
    fn drop(&mut self) {
        self.parked.take();
        self.pool.clean_parked(&self.key);
    }
}

struct Expiration(Option<Duration>);

impl Expiration {
    fn new(dur: Option<Duration>) -> Expiration {
        Expiration(dur)
    }

    fn expires(&self, instant: Instant) -> bool {
        match self.0 {
            Some(timeout) => instant.elapsed() > timeout,
            None => false,
        }
    }
}


/*
#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;
    use futures::{Async, Future, Poll};
    use futures::future;
    use proto::KeepAlive;
    use super::{Ready, Pool};

    impl Ready for i32 {
        fn poll_ready(&mut self) -> Poll<(), ()> {
            Ok(Async::Ready(()))
        }
    }

    #[test]
    fn test_pool_checkout_smoke() {
        let pool = Pool::new(true, Some(Duration::from_secs(5)));
        let key = Rc::new("foo".to_string());
        let mut pooled = pool.pooled(key.clone(), 41);
        pooled.idle();

        match pool.checkout(&key).poll().unwrap() {
            Async::Ready(pooled) => assert_eq!(*pooled, 41),
            _ => panic!("not ready"),
        }
    }

    #[test]
    fn test_pool_checkout_returns_none_if_expired() {
        future::lazy(|| {
            let pool = Pool::new(true, Some(Duration::from_secs(1)));
            let key = Rc::new("foo".to_string());
            let mut pooled = pool.pooled(key.clone(), 41);
            pooled.idle();
            ::std::thread::sleep(pool.inner.borrow().timeout.unwrap());
            assert!(pool.checkout(&key).poll().unwrap().is_not_ready());
            ::futures::future::ok::<(), ()>(())
        }).wait().unwrap();
    }

    #[test]
    fn test_pool_removes_expired() {
        let pool = Pool::new(true, Some(Duration::from_secs(1)));
        let key = Rc::new("foo".to_string());

        let mut pooled1 = pool.pooled(key.clone(), 41);
        pooled1.idle();
        let mut pooled2 = pool.pooled(key.clone(), 5);
        pooled2.idle();
        let mut pooled3 = pool.pooled(key.clone(), 99);
        pooled3.idle();


        assert_eq!(pool.inner.borrow().idle.get(&key).map(|entries| entries.len()), Some(3));
        ::std::thread::sleep(pool.inner.borrow().timeout.unwrap());

        pooled1.idle();
        pooled2.idle(); // idle after sleep, not expired
        pool.checkout(&key).poll().unwrap();
        assert_eq!(pool.inner.borrow().idle.get(&key).map(|entries| entries.len()), Some(1));
        pool.checkout(&key).poll().unwrap();
        assert!(pool.inner.borrow().idle.get(&key).is_none());
    }

    #[test]
    fn test_pool_checkout_task_unparked() {
        let pool = Pool::new(true, Some(Duration::from_secs(10)));
        let key = Rc::new("foo".to_string());
        let pooled1 = pool.pooled(key.clone(), 41);

        let mut pooled = pooled1.clone();
        let checkout = pool.checkout(&key).join(future::lazy(move || {
            // the checkout future will park first,
            // and then this lazy future will be polled, which will insert
            // the pooled back into the pool
            //
            // this test makes sure that doing so will unpark the checkout
            pooled.idle();
            Ok(())
        })).map(|(entry, _)| entry);
        assert_eq!(*checkout.wait().unwrap(), *pooled1);
    }

    #[test]
    fn test_pool_checkout_drop_cleans_up_parked() {
        future::lazy(|| {
            let pool = Pool::new(true, Some(Duration::from_secs(10)));
            let key = Rc::new("localhost:12345".to_string());
            let _pooled1 = pool.pooled(key.clone(), 41);
            let mut checkout1 = pool.checkout(&key);
            let mut checkout2 = pool.checkout(&key);

            // first poll needed to get into Pool's parked
            checkout1.poll().unwrap();
            assert_eq!(pool.inner.borrow().parked.get(&key).unwrap().len(), 1);
            checkout2.poll().unwrap();
            assert_eq!(pool.inner.borrow().parked.get(&key).unwrap().len(), 2);

            // on drop, clean up Pool
            drop(checkout1);
            assert_eq!(pool.inner.borrow().parked.get(&key).unwrap().len(), 1);

            drop(checkout2);
            assert!(pool.inner.borrow().parked.get(&key).is_none());

            ::futures::future::ok::<(), ()>(())
        }).wait().unwrap();
    }
}
*/
