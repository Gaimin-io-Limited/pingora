use super::*;
use crate::proxy_trait::ProxyHttp;
use log::warn;
use pingora_core::connectors::TransportConnector;
use pingora_core::protocols::http::connect_tunnel::ConnectDestination;
use pingora_core::protocols::l4::socket::SocketAddr;
use pingora_core::upstreams::peer::BasicPeer;
use pingora_error::{Error, Result};
use pingora_http::ResponseHeader;

impl<SV> HttpProxy<SV> {
    #[cfg(feature = "forward")]
    pub(crate) async fn handle_connect_request(
        &self,
        session: Session,
        ctx: &mut SV::CTX,
    ) -> Option<Stream>
    where
        SV: ProxyHttp + Send + Sync + 'static,
        SV::CTX: Send + Sync,
    {
        debug!("Handling CONNECT request");
        // take ownership of the session but ensure it is mutable
        let mut session = session;
        session.set_keepalive(Some(3));

        // for CONNECT requests through a proxy, the target destination is typically in the Host header
        // or in the URI path (e.g., CONNECT target.com:443 HTTP/1.1)
        let req_header = session.req_header();
        let destination = match (req_header.headers.get("host"), req_header.uri.authority()) {
            // try to parse from Host header first (common for proxy CONNECT requests)
            (Some(host_header), _) => match host_header.to_str() {
                Ok(host_str) => match ConnectDestination::parse(host_str) {
                    Ok(dest) => dest,
                    Err(e) => {
                        return self
                            .handle_connect_error(
                                &mut session,
                                ctx,
                                Error::e_explain(InvalidHTTPHeader, e.to_string()),
                                400,
                            )
                            .await;
                    }
                },
                Err(_) => {
                    return self
                        .handle_connect_error(
                            &mut session,
                            ctx,
                            Error::e_explain(
                                InvalidHTTPHeader,
                                "Invalid Host header in CONNECT request",
                            ),
                            400,
                        )
                        .await;
                }
            },
            // Fall back to URI authority if no Host header
            (None, Some(auth)) => match ConnectDestination::parse(auth.as_str()) {
                Ok(dest) => dest,
                Err(e) => {
                    return self
                        .handle_connect_error(
                            &mut session,
                            ctx,
                            Error::e_explain(InvalidHTTPHeader, e.to_string()),
                            400,
                        )
                        .await;
                }
            },
            (None, None) => {
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(
                            HTTPStatus(400),
                            "CONNECT requests must include a Host header or URI authority",
                        ),
                        400,
                    )
                    .await;
            }
        };

        debug!("Parsed CONNECT destination: {}", destination);

        // call user filter to validate and get upstream peer
        match self
            .inner
            .connect_request_filter(&mut session, &destination, ctx)
            .await
        {
            Ok(true) => {
                debug!("CONNECT request accepted by filter");
            },
            Ok(false) => {
                debug!("CONNECT request rejected by filter");
                // rejected by filter
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(HTTPStatus(502), "CONNECT request forbidden"),
                        502,
                    )
                    .await;
            }
            Err(_) => {
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(HTTPStatus(403), "CONNECT request forbidden"),
                        403,
                    )
                    .await;
            }
        };

        debug!("establishing connection to {}", destination.to_string().as_str());

        // establish raw TCP connection to upstream
        // TODO(@siennathesane): fix this with cloudflare/pingora#687
        // TODO(@siennathesane): this will break with ipv6 addresses, need to handle that
        let addr = match destination.to_string().parse() {
            Ok(addr) => SocketAddr::Inet(addr),
            Err(e) => {
                debug!("Failed to parse address: {}", e);
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(Custom("this is broken with IPv6. if using IPv4, try again"), e.to_string()),
                        500,
                    )
                    .await;
            }
        };
        debug!("parsed address to {}", addr.to_string().as_str());
        let peer = BasicPeer::new(addr.to_string().as_str());

        let transport = TransportConnector::new(Some(ConnectorOptions::new(1)));
        let mut upstream_stream = match transport.new_stream(&peer).await {
            Ok(s) => {
                debug!("Successfully connected to upstream: {}", addr);
                s },
            Err(e) => {
                debug!("failed to connect to upstream: {}", e);
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(HTTPStatus(502), "Failed to connect upstream"),
                        502,
                    )
                    .await;
            }
        };

        // send 200 Connection Established response
        let success_resp = match ResponseHeader::build(200, Some(3)) {
            Ok(mut resp) => {
                match resp.insert_header("Connection", "keep-alive") {
                    Ok(_) => {}
                    Err(_) => {
                        debug!("Failed to insert Connection header");
                        return self
                            .handle_connect_error(
                                &mut session,
                                ctx,
                                Error::e_explain(HTTPStatus(500), "Failed to insert header"),
                                500,
                            )
                            .await;
                    }
                };
                resp
            }
            Err(_) => {
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(HTTPStatus(500), "Failed to build response header"),
                        500,
                    )
                    .await;
            }
        };
        debug!("Sending 200 Connection Established response to client");

        match session
            .write_response_header(Box::new(success_resp), false)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                debug!("Failed to write response header: {}", e);
                return self
                    .handle_connect_error(
                        &mut session,
                        ctx,
                        Error::e_explain(HTTPStatus(500), "Failed to write response header"),
                        500,
                    )
                    .await;
            }
        };

        if let Err(e) = self
            .inner
            .connect_tunnel_established(&mut session, &destination, ctx)
            .await
        {
            warn!("Error in connect_tunnel_established callback: {}", e);
        }

        // Extract the client stream by finishing the HTTP session (transition from HTTP to raw TCP)
        let inner = session.downstream_session;
        let mut client_stream = match inner.finish().await {
            Ok(Some(stream)) => stream,
            Ok(None) => {
                debug!("No client stream available, nothing to do");
                return None; // No client stream available, nothing to do
            }
            Err(_) => {
                debug!("Failed to finish session, no client stream available");
                return None;
            }
        };

        debug!("Client stream established, starting bidirectional copy");

        // TODO(@siennathesane): i'm not sure if the spec has anything about what to do on connection end
        match tokio::io::copy_bidirectional(&mut client_stream, &mut upstream_stream).await {
            Ok((bytes_up, bytes_down)) => {
                debug!(
                    "{} bytes upstream, {} bytes downstream",
                    bytes_up, bytes_down
                );
                None
            }
            Err(e) => {
                warn!("Error during bidirectional copy: {}", e);
                None
            }
        }
    }

    async fn handle_connect_error(
        &self,
        session: &mut Session,
        ctx: &mut SV::CTX,
        error: Result<()>,
        status_code: u16,
    ) -> Option<Stream>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if session.response_written().is_none() {
            let error_response =
                ResponseHeader::build(status_code, None).expect("Failed to build error response");
            let _ = session
                .write_response_header(Box::new(error_response), true)
                .await;
        }

        self.inner
            .logging(session, Some(&error.unwrap_err()), ctx)
            .await;

        None
    }
}
