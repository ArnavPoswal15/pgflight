//! Client SSL/GSS probes receive a single-byte decline.

/// PostgreSQL declines SSL with `N` when not offering TLS on the proxy leg.
pub const SSL_DECLINE: u8 = b'N';

pub fn decline_bytes() -> [u8; 1] {
    [SSL_DECLINE]
}
