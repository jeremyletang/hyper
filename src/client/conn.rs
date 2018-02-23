//! dox
use std::fmt;
use std::marker::PhantomData;

use bytes::Bytes;
use futures::{Async, Future, Poll, Stream};
use futures::future::{self, Either};
use tokio_io::{AsyncRead, AsyncWrite};

use proto;
use super::{dispatch, Request, Response};

/// dox
pub fn handshake<T>(io: T) -> Handshake<T, ::Body>
where
    T: AsyncRead + AsyncWrite,
{
    Builder::new()
        .handshake(io)
}

/// dox
pub struct SendRequest<B> {
    dispatch: dispatch::Sender<proto::dispatch::ClientMsg<B>, ::Response>,

}

/// dox
pub struct Connection<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    inner: proto::dispatch::Dispatcher<
        proto::dispatch::Client<B>,
        B,
        T,
        B::Item,
        proto::ClientUpgradeTransaction,
    >,
}


/// dox
#[derive(Clone, Debug)]
pub struct Builder {
    h1_writev: bool,
}

/// dox
pub struct Handshake<T, B> {
    inner: HandshakeInner<T, B, proto::ClientUpgradeTransaction>,
}

/// dox
pub struct ResponseFuture {
    // for now, a Box is used to hide away the internal `B`
    // that can be returned if canceled
    inner: Box<Future<Item=Response, Error=::Error>>,
}

/// dox
#[derive(Debug)]
pub struct Parts<T> {
    /// dox
    pub io: T,
    /// dox
    pub read_buf: Bytes,
    _inner: (),
}

// internal client api

/*
pub(super) type ConnectionNoUpgrades<T, B> = proto::dispatch::Dispatcher<
    proto::dispatch::Client<B>,
    B,
    T,
    B::Item,
    proto::ClientUpgradeTransaction,
>;
*/

pub(super) struct HandshakeNoUpgrades<T, B> {
    inner: HandshakeInner<T, B, proto::ClientTransaction>,
}

struct HandshakeInner<T, B, R> {
    builder: Builder,
    io: Option<T>,
    _marker: PhantomData<(B, R)>,
}

// ===== impl SendRequest

impl<B> SendRequest<B>
{
    /// dox
    pub fn poll_ready(&mut self) -> Poll<(), ()> {
        if self.dispatch.is_closed() {
            //TODO: Err(::Error::Closed)
            Err(())
        } else {
            Ok(Async::Ready(()))
        }
    }
}

impl<B> SendRequest<B>
where
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    /// dox
    pub fn send_request(&mut self, req: Request<B>) -> ResponseFuture {
        let inner = self.send_request_retryable(req).map_err(|e| {
            let (err, _orig_req) = e;
            err
        });
        ResponseFuture {
            inner: Box::new(inner),
        }
    }

    pub(super) fn close(&self) {
        self.dispatch.cancel();
    }

    //TODO: replace with `impl Future` when stable
    pub(crate) fn send_request_retryable(&mut self, req: Request<B>) -> Box<Future<Item=Response, Error=(::Error, Option<(::proto::RequestHead, Option<B>)>)>> {
        let (head, body) = proto::request::split(req);
        let inner = match self.dispatch.send((head, body)) {
            Ok(rx) => {
                Either::A(rx.then(move |res| {
                    match res {
                        Ok(Ok(res)) => Ok(res),
                        Ok(Err(err)) => Err(err),
                        // this is definite bug if it happens, but it shouldn't happen!
                        Err(_) => panic!("dispatch dropped without returning error"),
                    }
                }))
            },
            Err(req) => {
                debug!("connection was not ready");
                let err = ::Error::new_canceled(None);
                Either::B(future::err((err, Some(req))))
            }
        };
        Box::new(inner)
    }
}

/*
impl<T, B> Service for SendRequest<T, B> {
    type Request = Request<B>;
    type Response = Response;
    type Error = ::Error;
    type Future = ResponseFuture;

    fn call(&self, req: Self::Request) -> Self::Future {

    }
}
*/

impl<B> fmt::Debug for SendRequest<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SendRequest")
            .finish()
    }
}

// ===== impl Connection

impl<T, B> Connection<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    /*
    pub fn into_inner(self) -> T {

    }
    */

    /// Return the inner IO object, and additional information.
    pub fn into_parts(self) -> Parts<T> {
        let (io, read_buf) = self.inner.into_inner();
        Parts {
            io: io,
            read_buf: read_buf,
            _inner: (),
        }
    }

    /// Runs the connection until completion, but without calling `shutdown`
    /// on the underlying IO.
    pub fn poll_upgrade(&mut self) -> Poll<(), ::Error> {
        self.inner.poll_without_shutdown()
    }
}

impl<T, B> Future for Connection<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    type Item = ();
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl<T, B> fmt::Debug for Connection<T, B>
where
    T: AsyncRead + AsyncWrite + fmt::Debug,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .finish()
    }
}

// ===== impl Builder

impl Builder {
    /// Creates a new connection builder.
    pub fn new() -> Builder {
        Builder {
            h1_writev: true,
        }
    }

    pub(super) fn h1_writev(&mut self, enabled: bool) -> &mut Builder {
        self.h1_writev = enabled;
        self
    }

    /// Constructs a connection with the configured options and IO.
    pub fn handshake<T, B>(&self, io: T) -> Handshake<T, B>
    where
        T: AsyncRead + AsyncWrite,
        B: Stream<Error=::Error> + 'static,
        B::Item: AsRef<[u8]>,
    {
        Handshake {
            inner: HandshakeInner {
                builder: self.clone(),
                io: Some(io),
                _marker: PhantomData,
            }
        }
    }

    pub(super) fn handshake_no_upgrades<T, B>(&self, io: T) -> HandshakeNoUpgrades<T, B>
    where
        T: AsyncRead + AsyncWrite,
        B: Stream<Error=::Error> + 'static,
        B::Item: AsRef<[u8]>,
    {
        HandshakeNoUpgrades {
            inner: HandshakeInner {
                builder: self.clone(),
                io: Some(io),
                _marker: PhantomData,
            }
        }
    }
}

// ===== impl Handshake

impl<T, B> Future for Handshake<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    type Item = (SendRequest<B>, Connection<T, B>);
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
            .map(|async| {
                async.map(|(tx, dispatch)| {
                    (tx, Connection { inner: dispatch })
                })
            })
    }
}

impl<T, B> fmt::Debug for Handshake<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Handshake")
            .finish()
    }
}

impl<T, B> Future for HandshakeNoUpgrades<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    type Item = (SendRequest<B>, proto::dispatch::Dispatcher<
        proto::dispatch::Client<B>,
        B,
        T,
        B::Item,
        proto::ClientTransaction,
    >);
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl<T, B, R> Future for HandshakeInner<T, B, R>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
    R: proto::Http1Transaction<
        Incoming=proto::RawStatus,
        Outgoing=proto::RequestLine,
    >,
{
    type Item = (SendRequest<B>, proto::dispatch::Dispatcher<
        proto::dispatch::Client<B>,
        B,
        T,
        B::Item,
        R,
    >);
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let io = self.io.take().expect("polled more than once");
        let (tx, rx) = dispatch::channel();
        let mut conn = proto::Conn::new(io);
        if !self.builder.h1_writev {
            conn.set_write_strategy_flatten();
        }
        let dispatch = proto::dispatch::Dispatcher::new(proto::dispatch::Client::new(rx), conn);
        Ok(Async::Ready((
            SendRequest {
                dispatch: tx,
            },
            dispatch,
        )))
    }
}

// ===== impl ResponseFuture

impl Future for ResponseFuture {
    type Item = Response;
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl fmt::Debug for ResponseFuture {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ResponseFuture")
            .finish()
    }
}

// assert trait markers

trait AssertSend: Send {}
trait AssertSendSync: Send + Sync {}


#[doc(hidden)]
impl<B: Send> AssertSendSync for SendRequest<B> {}

#[doc(hidden)]
impl<T: Send, B: Send> AssertSend for Connection<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: Stream<Error=::Error>,
    B::Item: AsRef<[u8]> + Send,
{}

#[doc(hidden)]
impl AssertSendSync for Builder {}
