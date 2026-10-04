use crate::{Limits, TrustedProxies};
use std::{fmt, net::SocketAddr};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub limits: Limits,
    pub trusted_proxies: TrustedProxies,
}
#[derive(Debug)]
pub struct ConfigError(String);
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ConfigError {}
impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::load(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err(ConfigError(format!("{name}: expected UTF-8"))),
        })
    }
    fn load(
        mut read: impl FnMut(&str) -> Result<Option<String>, ConfigError>,
    ) -> Result<Self, ConfigError> {
        let mut limits = Limits::default();
        macro_rules! limit {
            ($name:literal, $field:ident) => {
                if let Some(value) = read(concat!("PUZZELLA_RENDEZVOUS_", $name))? {
                    limits.$field = value.parse().map_err(|_| {
                        ConfigError(
                            concat!(
                                "PUZZELLA_RENDEZVOUS_",
                                $name,
                                ": expected a positive integer"
                            )
                            .into(),
                        )
                    })?;
                }
            };
        }
        limit!("MAX_CONNECTIONS", connections);
        limit!("MAX_CONNECTIONS_PER_IP", connections_per_ip);
        limit!(
            "MAX_ADMISSIONS_PER_IP_PER_MINUTE",
            admissions_per_ip_per_minute
        );
        limit!(
            "MAX_ROOM_ATTEMPTS_PER_IP_PER_MINUTE",
            room_attempts_per_ip_per_minute
        );
        limit!("MAX_ROOMS", rooms);
        limit!("MAX_PARTICIPANTS", participants);
        limit!("MAX_PENDING_JOINS", pending);
        limit!("MAX_OUTBOUND_MESSAGES", outbound);
        limit!(
            "MAX_OUTBOUND_BYTES_PER_CONNECTION",
            outbound_bytes_per_connection
        );
        limit!("MAX_OUTBOUND_BYTES_GLOBAL", outbound_bytes_global);
        limits
            .validate()
            .map_err(|reason| ConfigError(format!("invalid rendezvous limits: {reason}")))?;
        let listen = read("PUZZELLA_RENDEZVOUS_LISTEN")?
            .unwrap_or_else(|| "127.0.0.1:8080".into())
            .parse()
            .map_err(|_| {
                ConfigError("PUZZELLA_RENDEZVOUS_LISTEN: expected a numeric socket address".into())
            })?;
        let trusted_proxies = read("PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES")?
            .unwrap_or_default()
            .parse()
            .map_err(|reason| {
                ConfigError(format!("PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES: {reason}"))
            })?;
        Ok(Self {
            listen,
            limits,
            trusted_proxies,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::load(|name| {
            Ok(values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).into()))
        })
    }
    #[test]
    fn defaults_and_validated_deployment_limits() {
        let c = config(&[]).unwrap();
        assert_eq!(c.listen, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(c.limits.outbound_bytes_per_connection, 256 * 1024);
        assert_eq!(c.limits.outbound_bytes_global, 32 * 1024 * 1024);
        let c = config(&[
            ("PUZZELLA_RENDEZVOUS_LISTEN", "[::1]:9000"),
            (
                "PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES",
                "127.0.0.1/32,::1/128",
            ),
            ("PUZZELLA_RENDEZVOUS_MAX_CONNECTIONS", "1024"),
            ("PUZZELLA_RENDEZVOUS_MAX_CONNECTIONS_PER_IP", "1024"),
            (
                "PUZZELLA_RENDEZVOUS_MAX_ADMISSIONS_PER_IP_PER_MINUTE",
                "2048",
            ),
            (
                "PUZZELLA_RENDEZVOUS_MAX_ROOM_ATTEMPTS_PER_IP_PER_MINUTE",
                "4096",
            ),
            ("PUZZELLA_RENDEZVOUS_MAX_OUTBOUND_BYTES_GLOBAL", "67108864"),
        ])
        .unwrap();
        assert_eq!(c.limits.connections_per_ip, 1024);
        assert_eq!(c.limits.admissions_per_ip_per_minute, 2048);
        assert_eq!(c.limits.room_attempts_per_ip_per_minute, 4096);
        assert_eq!(c.limits.outbound_bytes_global, 64 * 1024 * 1024);
    }
    #[test]
    fn invalid_configuration_fails_before_binding() {
        for values in [
            vec![("PUZZELLA_RENDEZVOUS_MAX_CONNECTIONS", "0")],
            vec![("PUZZELLA_RENDEZVOUS_MAX_CONNECTIONS", "1025")],
            vec![("PUZZELLA_RENDEZVOUS_MAX_CONNECTIONS_PER_IP", "1025")],
            vec![(
                "PUZZELLA_RENDEZVOUS_MAX_ADMISSIONS_PER_IP_PER_MINUTE",
                "65537",
            )],
            vec![("PUZZELLA_RENDEZVOUS_MAX_OUTBOUND_BYTES_GLOBAL", "67108865")],
            vec![(
                "PUZZELLA_RENDEZVOUS_MAX_OUTBOUND_BYTES_PER_CONNECTION",
                "-1",
            )],
            vec![("PUZZELLA_RENDEZVOUS_LISTEN", "localhost:8080")],
            vec![("PUZZELLA_RENDEZVOUS_TRUSTED_PROXIES", "0.0.0.0/0")],
        ] {
            assert!(config(&values).is_err(), "{values:?}");
        }
    }
}
