use pingora_error::{Error, ErrorType::*, Result};
use std::fmt::Display;
use std::net::ToSocketAddrs;

/// Represents a destination for a CONNECT request.
#[derive(Debug, Clone)]
pub struct ConnectDestination {
    pub host: String,
    pub port: u16,
}

impl ConnectDestination {
    /// Parses a ConnectDestination from an authority string (e.g., `example.com:80` or `[::1]:8080`).
    pub fn parse(authority: &str) -> Result<Self> {
        match authority.to_socket_addrs() {
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => Ok(Self {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                }),
                None => Error::e_explain(InvalidHTTPHeader, "No valid address found in authority"),
            },
            Err(e) => Error::e_explain(InvalidHTTPHeader, format!("Invalid authority: {}", e)),
        }
    }
}

impl Display for ConnectDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
