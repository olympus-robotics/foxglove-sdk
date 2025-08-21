use std::borrow::Cow;

use bytes::{Buf, BufMut};

use crate::websocket::ws_protocol::{BinaryMessage, ParseError};

use super::BinaryOpcode;

/// Service call request message.
///
/// Spec: <https://github.com/foxglove/ws-protocol/blob/main/docs/spec.md#service-call-request>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceCallRequest<'a> {
    /// Service ID.
    pub service_id: u32,
    /// Call ID.
    pub call_id: u32,
    /// Timeout in milliseconds for the service call. A value of 0 may be treated as no timeout.
    pub timeout_ms: u32,
    /// Encoding.
    pub encoding: Cow<'a, str>,
    /// Payload.
    pub payload: Cow<'a, [u8]>,
}

impl ServiceCallRequest<'_> {
    /// Returns an owned version of this message.
    pub fn into_owned(self) -> ServiceCallRequest<'static> {
        ServiceCallRequest {
            service_id: self.service_id,
            call_id: self.call_id,
            timeout_ms: self.timeout_ms,
            encoding: Cow::Owned(self.encoding.into_owned()),
            payload: Cow::Owned(self.payload.into_owned()),
        }
    }
}

impl<'a> BinaryMessage<'a> for ServiceCallRequest<'a> {
    fn parse_binary(mut data: &'a [u8]) -> Result<Self, ParseError> {
        // Need at least service id + call id + third field
        if data.remaining() < 4 + 4 + 4 {
            return Err(ParseError::BufferTooShort);
        }
        let service_id = data.get_u32_le();
        let call_id = data.get_u32_le();
        let third = data.get_u32_le();

        // Heuristic backward compatibility:
        // Old layout: service_id, call_id, encoding_len, <encoding>, <payload>
        // New layout: service_id, call_id, timeout_ms, encoding_len, <encoding>, <payload>
        // We have already consumed up to third. For new layout we still need 4 bytes for encoding_len.
        let mut timeout_ms = 0u32;
        let encoding_length: usize;
        if data.remaining() < 4 {
            // Not enough for new encoding length field => must be old layout.
            encoding_length = third as usize;
        } else {
            // Peek next 4 bytes without consuming.
            let next_encoding_len = {
                let mut tmp = &data[..4];
                tmp.get_u32_le()
            } as usize;
            let old_valid = (third as usize) <= data.remaining();
            let new_valid = data.remaining() >= 4 + next_encoding_len;
            if new_valid && (!old_valid
                // prefer new if both valid and timeout candidate implausible as encoding length
                || (old_valid && (third > 10_000 || next_encoding_len <= 64)))
            {
                // Treat third as timeout, consume encoding length
                timeout_ms = third;
                data.advance(4);
                encoding_length = next_encoding_len;
            } else {
                // Treat third as encoding length (old), leave timeout_ms = 0
                encoding_length = third as usize;
            }
        }
        if data.remaining() < encoding_length {
            return Err(ParseError::BufferTooShort);
        }
        let encoding = Cow::Borrowed(std::str::from_utf8(&data[..encoding_length])?);
        data.advance(encoding_length);
        Ok(Self {
            service_id,
            call_id,
            timeout_ms,
            encoding,
            payload: Cow::Borrowed(data),
        })
    }

    fn to_bytes(&self) -> Vec<u8> {
        let size = 1 + 4 + 4 + 4 + 4 + self.encoding.len() + self.payload.len();
        let mut buf = Vec::with_capacity(size);
        buf.put_u8(BinaryOpcode::ServiceCallRequest as u8);
        buf.put_u32_le(self.service_id);
        buf.put_u32_le(self.call_id);
        buf.put_u32_le(self.timeout_ms);
        buf.put_u32_le(self.encoding.len() as u32);
        buf.put_slice(self.encoding.as_bytes());
        buf.put_slice(&self.payload);
        buf
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use crate::websocket::ws_protocol::client::ClientMessage;

    use super::*;

    fn message() -> ServiceCallRequest<'static> {
        ServiceCallRequest {
            service_id: 10,
            call_id: 12,
            timeout_ms: 5_000,
            encoding: "json".into(),
            payload: br#"{"key": "value"}"#.into(),
        }
    }

    #[test]
    fn test_encode() {
        insta::assert_snapshot!(format!("{:#04x?}", message().to_bytes()));
    }

    #[test]
    fn test_parse() {
        assert_matches!(
            ServiceCallRequest::parse_binary(b""),
            Err(ParseError::BufferTooShort)
        );
        assert_matches!(
            ServiceCallRequest::parse_binary(&[0; 15]),
            Err(ParseError::BufferTooShort)
        );
        let mut buf = Vec::new();
        buf.put_u32_le(10);
        buf.put_u32_le(12);
        buf.put_u32_le(0);
        buf.put_u32_le(1);
        assert_matches!(
            ServiceCallRequest::parse_binary(&buf),
            Err(ParseError::BufferTooShort)
        );
    }

    #[test]
    fn test_roundtrip() {
        let orig = message();
        let buf = orig.to_bytes();
        let msg = ClientMessage::parse_binary(&buf).unwrap();
        assert_eq!(msg, ClientMessage::ServiceCallRequest(orig));
    }

    #[test]
    fn test_parse_legacy_without_timeout() {
        // legacy layout: service_id, call_id, encoding_len, encoding, payload
        let service_id = 42u32;
        let call_id = 7u32;
        let encoding = b"json"; // len 4
        let payload = br#"{}"#;
        let mut buf = Vec::new();
        buf.put_u8(super::BinaryOpcode::ServiceCallRequest as u8);
        buf.put_u32_le(service_id);
        buf.put_u32_le(call_id);
        buf.put_u32_le(encoding.len() as u32); // interpreted as encoding length (legacy)
        buf.extend_from_slice(encoding);
        buf.extend_from_slice(payload);
        let parsed = ClientMessage::parse_binary(&buf).unwrap();
        match parsed {
            ClientMessage::ServiceCallRequest(req) => {
                assert_eq!(req.service_id, service_id);
                assert_eq!(req.call_id, call_id);
                assert_eq!(req.timeout_ms, 0); // legacy => default
                assert_eq!(req.encoding, "json");
                assert_eq!(req.payload, payload);
            }
            _ => panic!("unexpected variant"),
        }
    }
}
