use crate::{
    turn::{budget::BudgetConfig, TurnConfig},
    Limits, TrustedProxies,
};
use std::{fmt, net::SocketAddr};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub turn: Option<TurnConfig>,
    pub turn_budget: Option<BudgetConfig>,
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
        let key_id = read("PUZZELLA_RENDEZVOUS_TURN_KEY_ID")?.filter(|v| !v.is_empty());
        let api_token = read("PUZZELLA_RENDEZVOUS_TURN_API_TOKEN")?.filter(|v| !v.is_empty());
        let mut number = |name, default, min, max| -> Result<u64, ConfigError> {
            let n = read(name)?
                .map(|v| v.parse::<u64>())
                .transpose()
                .map_err(|_| ConfigError(format!("{name}: expected an integer")))?
                .unwrap_or(default);
            if !(min..=max).contains(&n) {
                return Err(ConfigError(format!("{name}: out of range")));
            }
            Ok(n)
        };
        let ttl = number("PUZZELLA_RENDEZVOUS_TURN_TTL_SECONDS", 86400, 600, 172800)?;
        let concurrency = number("PUZZELLA_RENDEZVOUS_TURN_MAX_CONCURRENCY", 4, 1, 32)? as usize;
        let requests_per_minute =
            number("PUZZELLA_RENDEZVOUS_TURN_REQUESTS_PER_MINUTE", 120, 1, 4096)? as u32;
        let turn = match (key_id, api_token) {
            (None, None) => None,
            (Some(key_id), Some(api_token))
                if key_id.len() <= 128
                    && key_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                    && api_token.len() <= 4096
                    && api_token.bytes().all(|b| b.is_ascii_graphic()) =>
            {
                Some(TurnConfig {
                    key_id,
                    api_token,
                    ttl: std::time::Duration::from_secs(ttl),
                    concurrency,
                    requests_per_minute,
                })
            }
            _ => {
                return Err(ConfigError(
                    "TURN_KEY_ID and TURN_API_TOKEN must both be valid or both absent".into(),
                ))
            }
        };
        let enabled = read("PUZZELLA_RENDEZVOUS_TURN_BUDGET_ENABLED")?;
        let enabled_configured = enabled.is_some();
        let enabled = match enabled.as_deref() {
            None | Some("false") | Some("0") => false,
            Some("true") | Some("1") => true,
            _ => {
                return Err(ConfigError(
                    "TURN_BUDGET_ENABLED: expected true/false or 1/0".into(),
                ))
            }
        };
        let required_names = [
            "TURN_BUDGET_ACCOUNT_ID",
            "TURN_BUDGET_ANALYTICS_TOKEN",
            "TURN_BUDGET_SOFT_LIMIT_BYTES",
            "TURN_BUDGET_HARD_LIMIT_BYTES",
            "TURN_BUDGET_BILLING_ANCHOR",
            "TURN_BUDGET_REGISTRY_PATH",
        ];
        let optional_names = [
            "TURN_BUDGET_POLL_SECONDS",
            "TURN_BUDGET_STALE_SECONDS",
            "TURN_BUDGET_PERIOD_SECONDS",
        ];
        let mut budget_values = std::collections::BTreeMap::new();
        for name in required_names.into_iter().chain(optional_names) {
            if let Some(value) = read(&format!("PUZZELLA_RENDEZVOUS_{name}"))? {
                budget_values.insert(name, value);
            }
        }
        if !enabled && !budget_values.is_empty() && !enabled_configured {
            return Err(ConfigError(
                "TURN budget settings require TURN_BUDGET_ENABLED=true (or explicit false)".into(),
            ));
        }
        let turn_budget = if enabled {
            if turn.is_none() {
                return Err(ConfigError(
                    "TURN budget requires configured TURN credentials".into(),
                ));
            }
            let required = |name| {
                budget_values
                    .get(name)
                    .filter(|v| !v.is_empty())
                    .cloned()
                    .ok_or_else(|| {
                        ConfigError(format!("{name}: required when TURN budget is enabled"))
                    })
            };
            let account_id = required("TURN_BUDGET_ACCOUNT_ID")?;
            if account_id.len() != 32 || !account_id.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(ConfigError(
                    "TURN_BUDGET_ACCOUNT_ID: expected 32 hexadecimal characters".into(),
                ));
            }
            let analytics_token = required("TURN_BUDGET_ANALYTICS_TOKEN")?;
            if analytics_token.len() > 4096
                || !analytics_token.bytes().all(|b| b.is_ascii_graphic())
            {
                return Err(ConfigError(
                    "TURN_BUDGET_ANALYTICS_TOKEN: invalid token".into(),
                ));
            }
            let number = |name, default, min, max| -> Result<u64, ConfigError> {
                let n = budget_values
                    .get(name)
                    .map(|s| s.parse::<u64>())
                    .transpose()
                    .map_err(|_| ConfigError(format!("{name}: expected an integer")))?
                    .or(default)
                    .ok_or_else(|| ConfigError(format!("{name}: required")))?;
                if !(min..=max).contains(&n) {
                    return Err(ConfigError(format!("{name}: out of range")));
                }
                Ok(n)
            };
            let soft_limit_bytes = number("TURN_BUDGET_SOFT_LIMIT_BYTES", None, 1, u64::MAX)?;
            let hard_limit_bytes = number("TURN_BUDGET_HARD_LIMIT_BYTES", None, 1, u64::MAX)?;
            if soft_limit_bytes >= hard_limit_bytes {
                return Err(ConfigError(
                    "TURN budget requires soft limit < hard limit".into(),
                ));
            }
            let poll = number("TURN_BUDGET_POLL_SECONDS", Some(30), 1, 3600)?;
            let stale = number("TURN_BUDGET_STALE_SECONDS", Some(300), 1, 86400)?;
            if stale <= poll {
                return Err(ConfigError(
                    "TURN budget stale timeout must exceed polling interval".into(),
                ));
            }
            let billing_anchor =
                chrono::DateTime::parse_from_rfc3339(&required("TURN_BUDGET_BILLING_ANCHOR")?)
                    .map_err(|_| {
                        ConfigError("TURN_BUDGET_BILLING_ANCHOR: expected RFC3339".into())
                    })?
                    .with_timezone(&chrono::Utc);
            if billing_anchor.timestamp_subsec_nanos() != 0
                || !(1970..=9998).contains(&chrono::Datelike::year(&billing_anchor))
            {
                return Err(ConfigError(
                    "TURN_BUDGET_BILLING_ANCHOR: expected whole seconds, year 1970..9998".into(),
                ));
            }
            let period_seconds = budget_values
                .get("TURN_BUDGET_PERIOD_SECONDS")
                .map(|_| number("TURN_BUDGET_PERIOD_SECONDS", None, 60, 366 * 86400))
                .transpose()?;
            Some(BudgetConfig {
                account_id,
                analytics_token,
                soft_limit_bytes,
                hard_limit_bytes,
                polling_interval: std::time::Duration::from_secs(poll),
                stale_timeout: std::time::Duration::from_secs(stale),
                billing_anchor,
                period_seconds,
                registry_path: required("TURN_BUDGET_REGISTRY_PATH")?.into(),
            })
        } else {
            None
        };
        Ok(Self {
            listen,
            turn,
            turn_budget,
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
    fn turn_configuration_is_optional_bounded_and_secret_safe() {
        assert!(config(&[]).unwrap().turn.is_none());
        let values = [
            ("PUZZELLA_RENDEZVOUS_TURN_KEY_ID", "private-key"),
            ("PUZZELLA_RENDEZVOUS_TURN_API_TOKEN", "private-token"),
        ];
        let valid = config(&values).unwrap();
        assert_eq!(valid.turn.as_ref().unwrap().ttl.as_secs(), 86400);
        assert!(!format!("{valid:?}").contains("private-"));
        assert!(config(&values[..1]).is_err());
        for (key, value) in [
            ("PUZZELLA_RENDEZVOUS_TURN_TTL_SECONDS", "599"),
            ("PUZZELLA_RENDEZVOUS_TURN_TTL_SECONDS", "172801"),
            ("PUZZELLA_RENDEZVOUS_TURN_MAX_CONCURRENCY", "33"),
            ("PUZZELLA_RENDEZVOUS_TURN_REQUESTS_PER_MINUTE", "0"),
        ] {
            let mut bad = values.to_vec();
            bad.push((key, value));
            assert!(config(&bad).is_err());
        }
    }
    #[test]
    fn budget_configuration_requires_complete_consistent_settings_and_redacts_tokens() {
        let mut values = vec![
            ("PUZZELLA_RENDEZVOUS_TURN_KEY_ID", "key"),
            ("PUZZELLA_RENDEZVOUS_TURN_API_TOKEN", "private-turn-token"),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_ENABLED", "true"),
            (
                "PUZZELLA_RENDEZVOUS_TURN_BUDGET_ACCOUNT_ID",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                "PUZZELLA_RENDEZVOUS_TURN_BUDGET_ANALYTICS_TOKEN",
                "private-analytics-token",
            ),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_SOFT_LIMIT_BYTES", "100"),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_HARD_LIMIT_BYTES", "200"),
            (
                "PUZZELLA_RENDEZVOUS_TURN_BUDGET_BILLING_ANCHOR",
                "2026-01-17T12:00:00+09:00",
            ),
            (
                "PUZZELLA_RENDEZVOUS_TURN_BUDGET_REGISTRY_PATH",
                "registry.jsonl",
            ),
        ];
        let c = config(&values).unwrap();
        let b = c.turn_budget.as_ref().unwrap();
        assert_eq!(b.polling_interval.as_secs(), 30);
        assert_eq!(b.stale_timeout.as_secs(), 300);
        assert_eq!(b.billing_anchor.to_rfc3339(), "2026-01-17T03:00:00+00:00");
        assert!(!format!("{c:?}").contains("private-"));
        for index in 0..values.len() {
            let mut missing = values.clone();
            missing.remove(index);
            assert!(config(&missing).is_err());
        }
        for (key, invalid) in [
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_ENABLED", "yes"),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_ACCOUNT_ID", "invalid"),
            (
                "PUZZELLA_RENDEZVOUS_TURN_BUDGET_ANALYTICS_TOKEN",
                "invalid token",
            ),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_SOFT_LIMIT_BYTES", "200"),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_HARD_LIMIT_BYTES", "0"),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_POLL_SECONDS", "0"),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_STALE_SECONDS", "30"),
            (
                "PUZZELLA_RENDEZVOUS_TURN_BUDGET_BILLING_ANCHOR",
                "2026-01-01",
            ),
            ("PUZZELLA_RENDEZVOUS_TURN_BUDGET_PERIOD_SECONDS", "0"),
        ] {
            let mut bad = values.clone();
            bad.retain(|(k, _)| *k != key);
            bad.push((key, invalid));
            assert!(config(&bad).is_err());
        }
        values[2].1 = "false";
        assert!(config(&values).unwrap().turn_budget.is_none());
        assert!(config(&[]).unwrap().turn_budget.is_none());
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
