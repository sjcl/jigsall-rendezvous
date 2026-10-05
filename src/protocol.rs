//! Mirrored in sjcl/jigsall game/src/network/gns/rendezvous/protocol.rs.
//! Change the canonical fixtures and both copies together; no gameplay data here.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

pub const MAX_SIGNAL_BYTES: usize = 16 * 1024;
pub const MAX_WS_BYTES: usize = 24 * 1024;
pub const MAX_BASE64_BYTES: usize = MAX_SIGNAL_BYTES.div_ceil(3) * 4;

/// A numeric version that refuses every value except 1 during parsing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V1;
impl Serialize for V1 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u16(1)
    }
}
impl<'de> Deserialize<'de> for V1 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        if u16::deserialize(d)? != 1 {
            return Err(serde::de::Error::custom("unsupported_version"));
        }
        Ok(Self)
    }
}
macro_rules! id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; 16]);
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&uuid::Uuid::from_bytes(self.0).hyphenated().to_string())
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let text = String::deserialize(d)?;
                let value = uuid::Uuid::parse_str(&text).map_err(serde::de::Error::custom)?;
                if value.is_nil() || value.hyphenated().to_string() != text {
                    return Err(serde::de::Error::custom("noncanonical_id"));
                }
                Ok(Self(*value.as_bytes()))
            }
        }
    };
}
id!(AuthorityId);
id!(RoomId);
id!(MemberId);
id!(PeerId);
id!(JoinId);

pub const CODE_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RoomCode(String);
impl FromStr for RoomCode {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.to_ascii_uppercase();
        if upper.len() != 10 || !upper.bytes().all(|c| CODE_ALPHABET.contains(&c)) {
            return Err("invalid_room_code");
        }
        Ok(Self(upper))
    }
}
impl fmt::Display for RoomCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl<'de> Deserialize<'de> for RoomCode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// UDP TURN endpoints only. Values are short-lived, never provider secrets.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnServer {
    pub address: String,
    pub username: String,
    pub password: String,
}
impl fmt::Debug for TurnServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnServer")
            .field("address", &self.address)
            .field("username", &"[redacted]")
            .field("password", &"[redacted]")
            .finish()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnCredentials {
    pub expires_at_unix: u64,
    pub servers: Vec<TurnServer>,
}
impl TurnCredentials {
    /// Rotation changes credentials, never the initially configured endpoint set.
    pub fn same_server_set(&self, other: &Self) -> bool {
        self.servers.len() == other.servers.len()
            && self
                .servers
                .iter()
                .all(|s| other.servers.iter().any(|o| o.address == s.address))
    }
    pub fn validate(&self) -> Result<(), ErrorCode> {
        if self.expires_at_unix == 0 || self.servers.is_empty() || self.servers.len() > 4 {
            return Err(ErrorCode::InvalidMessage);
        }
        for server in &self.servers {
            let Some((host, port)) = server.address.rsplit_once(':') else {
                return Err(ErrorCode::InvalidMessage);
            };
            if host.is_empty()
                || server.address.len() > 256
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-:[]".contains(&b))
                || !port.parse::<u16>().is_ok_and(|p| p != 0)
                || [server.username.as_str(), server.password.as_str()]
                    .iter()
                    .any(|s| {
                        s.is_empty()
                            || s.len() > 256
                            || !s.bytes().all(|b| b.is_ascii_graphic() && b != b',')
                    })
            {
                return Err(ErrorCode::InvalidMessage);
            }
        }
        if self
            .servers
            .iter()
            .enumerate()
            .any(|(i, s)| self.servers[..i].iter().any(|x| x.address == s.address))
        {
            return Err(ErrorCode::InvalidMessage);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame<M> {
    pub v: V1,
    #[serde(flatten)]
    pub message: M,
}
impl<M> Frame<M> {
    pub fn new(message: M) -> Self {
        Self { v: V1, message }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientMessage {
    CreateRoom {
        peer_id: PeerId,
    },
    JoinRoom {
        room_code: RoomCode,
        peer_id: PeerId,
    },
    AuthorizeAck {
        join_id: JoinId,
    },
    AuthorizeReject {
        join_id: JoinId,
    },
    ConfirmPeer {
        peer_id: PeerId,
        member_id: MemberId,
    },
    RevokePeer {
        peer_id: PeerId,
        member_id: MemberId,
    },
    Signal {
        to_peer_id: PeerId,
        payload_base64: String,
    },
    LeaveRoom {},
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerMessage {
    Welcome {
        authority_id: AuthorityId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnCredentials>,
    },
    TurnCredentials {
        turn: TurnCredentials,
    },
    TurnUnavailable {},
    RoomCreated {
        room_id: RoomId,
        room_code: RoomCode,
        self_member_id: MemberId,
    },
    AuthorizePeer {
        join_id: JoinId,
        peer_id: PeerId,
        member_id: MemberId,
    },
    RoomJoined {
        room_id: RoomId,
        self_member_id: MemberId,
        host_peer_id: PeerId,
        host_member_id: MemberId,
    },
    PeerJoined {
        peer_id: PeerId,
        member_id: MemberId,
    },
    /// Control routing is gone. This is NOT a GNS/gameplay disconnect.
    PeerUnavailable {
        peer_id: PeerId,
    },
    Signal {
        from_peer_id: PeerId,
        payload_base64: String,
    },
    RoomClosed {},
    Error {
        code: ErrorCode,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ProtocolViolation,
    UnsupportedVersion,
    InvalidMessage,
    NotInRoom,
    UnknownRoom,
    RoomFull,
    Capacity,
    DuplicatePeer,
    UnknownTarget,
    InvalidSignal,
    SignalTooLarge,
    RateLimited,
    JoinTimeout,
    Backpressure,
}
pub fn decode_signal(text: &str) -> Result<Vec<u8>, ErrorCode> {
    if text.len() > MAX_BASE64_BYTES {
        return Err(ErrorCode::SignalTooLarge);
    }
    let bytes = STANDARD
        .decode(text)
        .map_err(|_| ErrorCode::InvalidSignal)?;
    if bytes.len() > MAX_SIGNAL_BYTES {
        return Err(ErrorCode::SignalTooLarge);
    }
    if bytes.is_empty() || STANDARD.encode(&bytes) != text {
        return Err(ErrorCode::InvalidSignal);
    }
    Ok(bytes)
}
pub fn encode_signal(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}
pub fn parse_client(text: &str) -> Result<ClientMessage, ErrorCode> {
    parse::<ClientMessage>(text)
}
pub fn parse_server(text: &str) -> Result<ServerMessage, ErrorCode> {
    let message = parse::<ServerMessage>(text)?;
    match &message {
        ServerMessage::Welcome {
            turn: Some(turn), ..
        }
        | ServerMessage::TurnCredentials { turn } => turn.validate()?,
        _ => {}
    }
    Ok(message)
}
fn parse<M: serde::de::DeserializeOwned>(text: &str) -> Result<M, ErrorCode> {
    if text.len() > MAX_WS_BYTES {
        return Err(ErrorCode::InvalidMessage);
    }
    // First inspect only the version; typed parsing below rejects unknown types,
    // duplicate fields, untrusted identity fields and noncanonical IDs.
    #[derive(Deserialize)]
    struct Version {
        v: u16,
    }
    let version: Version = serde_json::from_str(text).map_err(|_| ErrorCode::InvalidMessage)?;
    if version.v != 1 {
        return Err(ErrorCode::UnsupportedVersion);
    }
    serde_json::from_str::<Frame<M>>(text)
        .map(|frame| frame.message)
        .map_err(|_| ErrorCode::InvalidMessage)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authorization_rejection_requires_only_exact_join_identity() {
        let valid =
            r#"{"v":1,"type":"authorize_reject","join_id":"66666666-6666-4666-8666-666666666666"}"#;
        assert!(matches!(
            parse_client(valid),
            Ok(ClientMessage::AuthorizeReject { .. })
        ));
        for invalid in [
            r#"{"v":1,"type":"authorize_reject"}"#.to_owned(),
            valid.replace(
                "66666666-6666-4666-8666-666666666666",
                "00000000-0000-0000-0000-000000000000",
            ),
            valid.replace(
                '}',
                ",\"peer_id\":\"55555555-5555-4555-8555-555555555555\"}",
            ),
            valid.replace('}', ",\"reason\":\"capacity\"}"),
        ] {
            assert_eq!(parse_client(&invalid), Err(ErrorCode::InvalidMessage));
        }
    }
    #[test]
    fn lifecycle_requires_exact_typed_identity_without_authority_fields() {
        for kind in ["confirm_peer", "revoke_peer"] {
            let valid = format!(
                r#"{{"v":1,"type":"{kind}","peer_id":"55555555-5555-4555-8555-555555555555","member_id":"77777777-7777-4777-8777-777777777777"}}"#
            );
            assert!(parse_client(&valid).is_ok());
            for invalid in [
                valid.replace(
                    ",\"member_id\":\"77777777-7777-4777-8777-777777777777\"",
                    "",
                ),
                valid.replace(
                    "77777777-7777-4777-8777-777777777777",
                    "00000000-0000-0000-0000-000000000000",
                ),
                valid.replace(
                    '}',
                    ",\"room_id\":\"33333333-3333-4333-8333-333333333333\"}",
                ),
                valid.replace('}', ",\"authenticated\":true}"),
            ] {
                assert_eq!(parse_client(&invalid), Err(ErrorCode::InvalidMessage));
            }
        }
    }
    #[test]
    fn turn_schema_is_bounded_and_debug_redacts_credentials() {
        let turn = TurnCredentials {
            expires_at_unix: 2000000000,
            servers: vec![TurnServer {
                address: "turn.cloudflare.com:3478".into(),
                username: "private-user".into(),
                password: "private-password".into(),
            }],
        };
        assert!(turn.validate().is_ok());
        for message in [
            ServerMessage::Welcome {
                authority_id: AuthorityId([1; 16]),
                turn: Some(turn.clone()),
            },
            ServerMessage::TurnCredentials { turn: turn.clone() },
        ] {
            assert!(!format!("{message:?}").contains("private-"));
            let json = serde_json::to_string(&Frame::new(message)).unwrap();
            assert!(parse_server(&json).is_ok());
            assert!(parse_client(&json).is_err()); // No client-driven credential minting.
        }
        let mut invalid = turn.clone();
        invalid.servers[0].username = "comma,value".into();
        assert!(invalid.validate().is_err());
        invalid = turn.clone();
        invalid.servers[0].address = "turn:turn.cloudflare.com:3478?transport=tcp".into();
        assert!(invalid.validate().is_err());
        invalid = turn.clone();
        invalid.servers = vec![turn.servers[0].clone(); 5];
        assert!(invalid.validate().is_err());
    }
    #[test]
    fn golden_protocol_v1() {
        for line in include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/protocol_v1.jsonl"
        ))
        .lines()
        {
            let (direction, json) = line.split_once(' ').unwrap();
            let encoded = if direction == "C" {
                serde_json::to_string(&Frame::new(parse_client(json).unwrap())).unwrap()
            } else {
                serde_json::to_string(&Frame::new(parse_server(json).unwrap())).unwrap()
            };
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
                serde_json::from_str::<serde_json::Value>(json).unwrap()
            );
        }
    }
    #[test]
    fn strict_schema_and_signal_limits() {
        assert_eq!(
            parse_client(r#"{"v":2,"type":"leave_room"}"#),
            Err(ErrorCode::UnsupportedVersion)
        );
        for bad in [
            r#"{"v":1,"type":"unknown"}"#,
            r#"{"v":1,"type":"leave_room","from_peer_id":"spoof"}"#,
            r#"{"v":1,"v":1,"type":"leave_room"}"#,
        ] {
            assert!(parse_client(bad).is_err());
        }
        assert_eq!(decode_signal("*"), Err(ErrorCode::InvalidSignal));
        assert_eq!(
            decode_signal(&encode_signal(&vec![1; MAX_SIGNAL_BYTES + 1])),
            Err(ErrorCode::SignalTooLarge)
        );
        assert_eq!(
            decode_signal(&encode_signal(&vec![1; MAX_SIGNAL_BYTES]))
                .unwrap()
                .len(),
            MAX_SIGNAL_BYTES
        );
        assert_eq!(
            "abcdefghjk".parse::<RoomCode>().unwrap().to_string(),
            "ABCDEFGHJK"
        );
        assert!("IIIIIIIIII".parse::<RoomCode>().is_err());
    }
}
