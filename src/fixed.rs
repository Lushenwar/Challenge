//! Exact decimal strings ⇄ i64 fixed point with 6 decimal places (1_000_000 = $1 / 1 contract).

pub const SCALE: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FixedError {
    #[error("empty decimal")]
    Empty,
    #[error("invalid decimal")]
    Invalid,
    #[error("more than 6 significant fractional digits")]
    TooPrecise,
    #[error("decimal overflows i64 fixed point")]
    Overflow,
}

/// Parse `"0.4500"`, `"-12.00"`, `"3"` into fixed point. Rejects >6 non-zero fractional
/// digits, exponents, whitespace, signs other than a leading `-`, empty strings and overflow.
pub fn parse_fixed(s: &str) -> Result<i64, FixedError> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() && frac.is_empty() {
        return Err(FixedError::Empty);
    }
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return Err(FixedError::Invalid);
    }
    let (kept, dropped) = frac.split_at(frac.len().min(6));
    if dropped.bytes().any(|b| b != b'0') {
        return Err(FixedError::TooPrecise);
    }
    let int_v: i64 = if int.is_empty() {
        0
    } else {
        int.parse().map_err(|_| FixedError::Overflow)?
    };
    // `kept` is 1..=6 ASCII digits here, so this parse cannot fail.
    let mut frac_v: i64 = if kept.is_empty() {
        0
    } else {
        kept.parse().unwrap()
    };
    for _ in kept.len()..6 {
        frac_v *= 10;
    }
    let v = int_v
        .checked_mul(SCALE)
        .and_then(|x| x.checked_add(frac_v))
        .ok_or(FixedError::Overflow)?;
    Ok(if neg { -v } else { v })
}

/// Inverse of [`parse_fixed`]: always 6 fractional digits.
pub fn format_fixed(v: i64) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let a = v.unsigned_abs();
    let scale = SCALE as u64;
    format!("{sign}{}.{:06}", a / scale, a % scale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_exact_decimals() {
        assert_eq!(parse_fixed("0.4500"), Ok(450_000));
        assert_eq!(parse_fixed("0.29"), Ok(290_000)); // a float parse would give 289_999
        assert_eq!(parse_fixed("-12.00"), Ok(-12_000_000));
        assert_eq!(parse_fixed("3"), Ok(3_000_000));
        assert_eq!(parse_fixed(".5"), Ok(500_000));
        assert_eq!(parse_fixed("5."), Ok(5_000_000));
        assert_eq!(parse_fixed("26794.95"), Ok(26_794_950_000));
        assert_eq!(parse_fixed("0.1234560000"), Ok(123_456));
    }

    #[test]
    fn formats_six_fractional_digits() {
        assert_eq!(format_fixed(450_000), "0.450000");
        assert_eq!(format_fixed(-1), "-0.000001");
        assert_eq!(format_fixed(0), "0.000000");
        assert_eq!(format_fixed(26_794_950_000), "26794.950000");
        assert_eq!(format_fixed(i64::MIN + 1), "-9223372036854.775807");
    }

    #[test]
    fn rejects_malformed() {
        for (s, e) in [
            ("", FixedError::Empty),
            (".", FixedError::Empty),
            ("-", FixedError::Empty),
            ("1e3", FixedError::Invalid),
            (" 1", FixedError::Invalid),
            ("+1", FixedError::Invalid),
            ("1.2.3", FixedError::Invalid),
            ("--1", FixedError::Invalid),
            ("0.1234567", FixedError::TooPrecise),
            ("9223372036854.775808", FixedError::Overflow),
            ("99999999999999999999", FixedError::Overflow),
        ] {
            assert_eq!(parse_fixed(s), Err(e), "{s:?}");
        }
    }

    proptest! {
        #[test]
        fn round_trips(v in -i64::MAX..=i64::MAX) {
            prop_assert_eq!(parse_fixed(&format_fixed(v)), Ok(v));
        }
    }
}
