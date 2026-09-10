//! Mango Protocol: wire types and codec for MangoStudio hubs, runtimes and tools.
//!
//! The wire types land with the spec in the next commits.

/// Wire major version this crate speaks.
pub const PROTOCOL_MAJOR: u16 = 1;
/// Highest wire minor version this crate speaks.
pub const PROTOCOL_MINOR: u16 = 0;

#[cfg(test)]
mod tests {
    use super::{PROTOCOL_MAJOR, PROTOCOL_MINOR};

    #[test]
    fn starts_at_wire_one_zero() {
        assert_eq!(PROTOCOL_MAJOR, 1);
        assert_eq!(PROTOCOL_MINOR, 0);
    }
}
