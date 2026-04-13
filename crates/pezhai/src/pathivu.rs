//! WAL and checksum helpers shared by the current foundation and later durable code paths.

/// Maximum accepted key size for public engine and server operations.
pub(crate) const MAX_KEY_BYTES: usize = 1024;

/// Maximum accepted value size for public engine and server operations.
pub(crate) const MAX_VALUE_BYTES: usize = 268_435_455;

/// Upper bound used by config validation for one encoded WAL record.
pub(crate) const MAX_SINGLE_WAL_RECORD_BYTES: u64 =
    64 + 8 + MAX_KEY_BYTES as u64 + MAX_VALUE_BYTES as u64 + 8;

/// Computes the CRC32C checksum mandated by the storage-engine specification.
#[allow(dead_code)]
#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(bytes)
}

#[cfg(test)]
mod tests {
    use super::{MAX_SINGLE_WAL_RECORD_BYTES, crc32c};

    #[test]
    fn crc32c_matches_the_standard_test_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn max_single_wal_record_bytes_matches_the_spec_formula() {
        assert_eq!(MAX_SINGLE_WAL_RECORD_BYTES, 268_436_559);
    }
}
