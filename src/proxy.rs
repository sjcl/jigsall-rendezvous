//! Forwarding headers are meaningful only behind explicitly configured proxies.
use axum::http::{header::HeaderName, HeaderMap};
use ipnet::IpNet;
use std::{net::IpAddr, str::FromStr};

const MAX_PROXIES: usize = 32;
const MAX_XFF_BYTES: usize = 1024;
const MAX_HOPS: usize = 16;

#[derive(Clone, Debug, Default)]
pub struct TrustedProxies(Vec<IpNet>);
impl FromStr for TrustedProxies {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.trim().is_empty() {
            return Ok(Self::default());
        }
        let mut nets = Vec::new();
        for entry in value.split(',') {
            if nets.len() >= MAX_PROXIES || !entry.contains('/') {
                return Err("expected at most 32 explicit proxy CIDRs");
            }
            let net = entry
                .trim()
                .parse::<IpNet>()
                .map_err(|_| "invalid proxy CIDR")?;
            if net.prefix_len() == 0 || net != net.trunc() {
                return Err("proxy CIDR must be a canonical network with a nonzero prefix");
            }
            // Use IPv4 CIDRs for mapped IPv4 addresses, just as admission does.
            if matches!(net, IpNet::V6(n) if n.addr().to_ipv4_mapped().is_some()) {
                return Err("use an IPv4 CIDR for IPv4-mapped addresses");
            }
            nets.push(net);
        }
        Ok(Self(nets))
    }
}
pub(crate) fn normalize(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        _ => ip,
    }
}
/// Apply only after TrustedProxies::source verifies the full source address.
pub(crate) fn prefix(ip: IpAddr) -> IpAddr {
    match normalize(ip) {
        IpAddr::V6(ip) => IpAddr::V6(std::net::Ipv6Addr::from(u128::from(ip) & (u128::MAX << 64))),
        ip => ip,
    }
}
impl TrustedProxies {
    fn contains(&self, ip: IpAddr) -> bool {
        self.0.iter().any(|net| net.contains(&normalize(ip)))
    }
    pub(crate) fn source(&self, peer: IpAddr, headers: &HeaderMap) -> Result<IpAddr, ()> {
        let peer = normalize(peer);
        if !self.contains(peer) {
            // Untrusted peers cannot affect IP guards through any header.
            return Ok(peer);
        }
        let mut values = headers
            .get_all(HeaderName::from_static("x-forwarded-for"))
            .iter();
        let value = values.next().ok_or(())?;
        if values.next().is_some() || value.as_bytes().len() > MAX_XFF_BYTES {
            return Err(());
        }
        let mut chain = Vec::new();
        for entry in value.to_str().map_err(|_| ())?.split(',') {
            if chain.len() >= MAX_HOPS {
                return Err(());
            }
            chain.push(normalize(entry.trim().parse::<IpAddr>().map_err(|_| ())?));
        }
        // Walk from the verified TCP peer toward the client. Never select the
        // spoofable leftmost prefix beyond the nearest untrusted hop.
        let mut source = peer;
        for hop in chain.into_iter().rev() {
            if !self.contains(source) {
                break;
            }
            source = hop;
        }
        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn headers(xff: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", xff.parse().unwrap());
        h
    }
    #[test]
    fn untrusted_headers_are_ignored_and_mapped_ipv4_shares_identity() {
        let proxies: TrustedProxies = "127.0.0.1/32,::1/128".parse().unwrap();
        let peer = "192.0.2.3".parse().unwrap();
        assert_eq!(proxies.source(peer, &headers("spoofed")), Ok(peer));
        assert_eq!(
            TrustedProxies::default().source(
                "::ffff:192.0.2.3".parse().unwrap(),
                &headers("198.51.100.7")
            ),
            Ok(peer)
        );
        assert_eq!(
            proxies.source(
                "::ffff:127.0.0.1".parse().unwrap(),
                &headers("::ffff:192.0.2.3")
            ),
            Ok(peer)
        );
    }
    #[test]
    fn trusted_chain_stops_at_nearest_untrusted_hop_and_ignores_other_headers() {
        let proxies: TrustedProxies = "127.0.0.1/32,::1/128,10.1.0.0/24".parse().unwrap();
        let mut h = headers("203.0.113.99, 198.51.100.7, 10.1.0.4");
        h.insert("forwarded", "for=203.0.113.88".parse().unwrap());
        h.insert("x-real-ip", "203.0.113.77".parse().unwrap());
        assert_eq!(
            proxies.source("127.0.0.1".parse().unwrap(), &h),
            Ok("198.51.100.7".parse().unwrap())
        );
        assert_eq!(
            proxies.source("::1".parse().unwrap(), &headers("2001:db8::7")),
            Ok("2001:db8::7".parse().unwrap())
        );
        assert_eq!(
            proxies.source("127.0.0.1".parse().unwrap(), &headers("127.0.0.1")),
            Ok("127.0.0.1".parse().unwrap())
        );
    }
    #[test]
    fn trusted_missing_malformed_duplicate_and_oversized_chains_fail_closed() {
        let proxies: TrustedProxies = "127.0.0.1/32".parse().unwrap();
        let peer = "127.0.0.1".parse().unwrap();
        assert!(proxies.source(peer, &HeaderMap::new()).is_err());
        for invalid in [
            "",
            "unknown",
            "192.0.2.1:80",
            "[2001:db8::1]",
            "192.0.2.1,",
            "garbage, 192.0.2.1",
        ] {
            assert!(proxies.source(peer, &headers(invalid)).is_err());
        }
        let mut duplicate = headers("192.0.2.1");
        duplicate.append("x-forwarded-for", "192.0.2.2".parse().unwrap());
        assert!(proxies.source(peer, &duplicate).is_err());
        assert!(proxies
            .source(peer, &headers(&"192.0.2.1,".repeat(17)))
            .is_err());
        assert!(proxies.source(peer, &headers(&" ".repeat(1025))).is_err());
    }
    #[test]
    fn invalid_or_overbroad_configuration_is_rejected() {
        for bad in [
            "127.0.0.1",
            "0.0.0.0/0",
            "::/0",
            "127.0.0.1/8",
            "::ffff:127.0.0.1/128",
            "127.0.0.1/32,",
        ] {
            assert!(bad.parse::<TrustedProxies>().is_err(), "{bad}");
        }
        assert!("127.0.0.1/32,"
            .repeat(33)
            .parse::<TrustedProxies>()
            .is_err());
    }
}
