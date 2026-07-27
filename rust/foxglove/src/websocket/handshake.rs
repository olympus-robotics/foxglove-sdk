use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::handshake::server;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{tungstenite, WebSocketStream};

pub(crate) const SUBPROTOCOL: &str = "foxglove.sdk.v1";

/// Query parameter by which a reconnecting client offers back the advertisement
/// token it was given last time.
const ADVERTISEMENT_TOKEN_PARAM: &str = "advertisement_token";

/// Outcome of a successful handshake.
pub(crate) struct Handshake<S> {
    pub stream: WebSocketStream<S>,
    /// Token offered by the client, if any. Clients that do not implement the
    /// extension never send it and are served normally.
    pub advertisement_token: Option<String>,
}

/// Extracts the advertisement token from a request query string.
fn parse_advertisement_token(query: Option<&str>) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == ADVERTISEMENT_TOKEN_PARAM && !value.is_empty()).then(|| value.to_string())
    })
}

/// Add the subprotocol header to the response if the client requested it. If the client requests
/// subprotocols which don't contain ours, or does not include the expected header, return a 400.
pub(crate) async fn do_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
) -> Result<Handshake<S>, tungstenite::Error> {
    let mut advertisement_token = None;
    let stream = tokio_tungstenite::accept_hdr_async(
        stream,
        |req: &server::Request, mut res: server::Response| {
            advertisement_token = parse_advertisement_token(req.uri().query());

            let protocol_headers = req.headers().get_all("sec-websocket-protocol");
            for header in &protocol_headers {
                if header
                    .to_str()
                    .unwrap_or_default()
                    .split(',')
                    .any(|v| v.trim() == SUBPROTOCOL)
                {
                    res.headers_mut().insert(
                        "sec-websocket-protocol",
                        HeaderValue::from_static(SUBPROTOCOL),
                    );
                    return Ok(res);
                }
            }

            let resp = server::Response::builder()
                .status(400)
                .body(Some(
                    "Missing expected sec-websocket-protocol header".into(),
                ))
                .unwrap();

            Err(resp)
        },
    )
    .await?;

    Ok(Handshake {
        stream,
        advertisement_token,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_advertisement_token;

    #[test]
    fn parses_token_from_query() {
        assert_eq!(
            parse_advertisement_token(Some("advertisement_token=abc123")),
            Some("abc123".to_string())
        );
        assert_eq!(
            parse_advertisement_token(Some("foo=1&advertisement_token=abc123&bar=2")),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn absent_or_empty_token_is_none() {
        assert_eq!(parse_advertisement_token(None), None);
        assert_eq!(parse_advertisement_token(Some("")), None);
        assert_eq!(parse_advertisement_token(Some("foo=1")), None);
        assert_eq!(
            parse_advertisement_token(Some("advertisement_token=")),
            None
        );
    }
}
