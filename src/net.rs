//! Opening websockets: TCP options, source-port pinning and connection racing.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::Context;
use tokio::net::{TcpSocket, TcpStream};
use tokio_tungstenite::tungstenite::handshake::client::Request;

pub type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

/// How sockets are opened.
#[derive(Clone, Debug)]
pub struct NetOpts {
    /// TCP_NODELAY: send our small frames (pings, commands) without Nagle's delay.
    pub nodelay: bool,
    /// Parallel handshakes per connect, spread over the host's addresses; the first to
    /// complete is kept, the rest are dropped mid-handshake. 1 = no racing.
    pub race: usize,
    /// Bind local port `base + conn * race + attempt`; any port if that one is taken.
    pub source_port_base: Option<u16>,
    /// Connect to this address instead of resolving the host (TLS still uses the host).
    pub ip: Option<IpAddr>,
}

impl Default for NetOpts {
    fn default() -> Self {
        Self {
            nodelay: true,
            race: 1,
            source_port_base: None,
            ip: None,
        }
    }
}

/// TCP (+ TLS for `wss`) + websocket handshake for connection `conn`. Returns the socket
/// and the peer address it reached.
pub async fn connect(
    req: Request,
    opts: &NetOpts,
    conn: usize,
) -> anyhow::Result<(Ws, SocketAddr)> {
    let host = req
        .uri()
        .host()
        .context("websocket URL has no host")?
        .to_owned();
    let tls = req.uri().scheme_str() == Some("wss");
    let port = req.uri().port_u16().unwrap_or(if tls { 443 } else { 80 });
    let addrs: Vec<SocketAddr> = match opts.ip {
        Some(ip) => vec![SocketAddr::new(ip, port)],
        None => tokio::net::lookup_host((host.as_str(), port))
            .await?
            .collect(),
    };
    anyhow::ensure!(!addrs.is_empty(), "no address for {host}");
    let race = opts.race.max(1);
    let attempts = (0..race).map(|i| {
        let addr = addrs[i % addrs.len()];
        let local = opts
            .source_port_base
            .map(|base| base.wrapping_add((conn * race + i) as u16));
        let req = clone_request(&req);
        Box::pin(open(req, addr, opts.nodelay, local))
    });
    // The losers are dropped here, which closes their sockets.
    let (won, _losers) = futures_util::future::select_ok(attempts).await?;
    Ok(won)
}

async fn open(
    req: anyhow::Result<Request>,
    addr: SocketAddr,
    nodelay: bool,
    local_port: Option<u16>,
) -> anyhow::Result<(Ws, SocketAddr)> {
    let tcp = match local_port {
        Some(p) => match bind_connect(addr, p).await {
            Ok(tcp) => tcp,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::AddrInUse | std::io::ErrorKind::AddrNotAvailable
                ) =>
            {
                tracing::debug!(port = p, "source port taken; using any port");
                TcpStream::connect(addr).await?
            }
            Err(e) => return Err(e.into()),
        },
        None => TcpStream::connect(addr).await?,
    };
    tcp.set_nodelay(nodelay)?;
    let (ws, _) = tokio_tungstenite::client_async_tls(req?, tcp).await?;
    Ok((ws, addr))
}

async fn bind_connect(addr: SocketAddr, port: u16) -> std::io::Result<TcpStream> {
    let (socket, any) = if addr.is_ipv4() {
        (TcpSocket::new_v4()?, IpAddr::V4(Ipv4Addr::UNSPECIFIED))
    } else {
        (TcpSocket::new_v6()?, IpAddr::V6(Ipv6Addr::UNSPECIFIED))
    };
    socket.bind(SocketAddr::new(any, port))?;
    socket.connect(addr).await
}

/// `Request` is not `Clone` (its extensions are not); rebuild it from the parts we set.
fn clone_request(req: &Request) -> anyhow::Result<Request> {
    let mut out = Request::builder()
        .method(req.method())
        .uri(req.uri())
        .version(req.version())
        .body(())?;
    *out.headers_mut() = req.headers().clone();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::{MaybeTlsStream, tungstenite::client::IntoClientRequest};

    /// A local websocket server; returns its URL. Counts accepted TCP connections.
    async fn server() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let n = accepted.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    if let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await {
                        use futures_util::StreamExt;
                        while ws.next().await.is_some() {}
                    }
                });
            }
        });
        (url, accepted)
    }

    fn tcp(ws: &Ws) -> &TcpStream {
        match ws.get_ref() {
            MaybeTlsStream::Plain(t) => t,
            _ => panic!("test server is plain ws"),
        }
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn sets_nodelay_and_pins_the_source_port() {
        let (url, _) = server().await;
        let base = free_port();
        let opts = NetOpts {
            source_port_base: Some(base),
            ..NetOpts::default()
        };
        let (ws, peer) = connect(url.as_str().into_client_request().unwrap(), &opts, 0)
            .await
            .unwrap();
        assert!(tcp(&ws).nodelay().unwrap());
        assert_eq!(tcp(&ws).local_addr().unwrap().port(), base);
        assert_eq!(peer.to_string(), url.trim_start_matches("ws://"));
        let off = NetOpts {
            nodelay: false,
            ..NetOpts::default()
        };
        let (ws, _) = connect(url.as_str().into_client_request().unwrap(), &off, 0)
            .await
            .unwrap();
        assert!(!tcp(&ws).nodelay().unwrap());
    }

    #[tokio::test]
    async fn a_taken_source_port_falls_back_to_any_port() {
        let (url, _) = server().await;
        let held = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let taken = held.local_addr().unwrap().port();
        let opts = NetOpts {
            source_port_base: Some(taken),
            ..NetOpts::default()
        };
        let (ws, _) = connect(url.as_str().into_client_request().unwrap(), &opts, 0)
            .await
            .unwrap();
        assert_ne!(tcp(&ws).local_addr().unwrap().port(), taken);
    }

    #[tokio::test]
    async fn racing_opens_several_and_keeps_one() {
        let (url, accepted) = server().await;
        let opts = NetOpts {
            race: 3,
            ..NetOpts::default()
        };
        let (mut ws, _) = connect(url.as_str().into_client_request().unwrap(), &opts, 0)
            .await
            .unwrap();
        use futures_util::SinkExt;
        ws.send(tokio_tungstenite::tungstenite::Message::Text("hi".into()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            accepted.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "three handshakes raced"
        );
    }
}
