pub mod auth;
pub mod discovery;

pub const REST_PROD: &str = "https://api.elections.kalshi.com/trade-api/v2";
pub const REST_DEMO: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
pub const WS_PROD: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
pub const WS_DEMO: &str = "wss://external-api.demo.kalshi.co/trade-api/ws/v2";
/// Path signed in the websocket handshake.
pub const WS_PATH: &str = "/trade-api/ws/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Env {
    Prod,
    Demo,
}

impl Env {
    pub fn rest(self) -> &'static str {
        match self {
            Self::Prod => REST_PROD,
            Self::Demo => REST_DEMO,
        }
    }

    pub fn ws(self) -> &'static str {
        match self {
            Self::Prod => WS_PROD,
            Self::Demo => WS_DEMO,
        }
    }
}
