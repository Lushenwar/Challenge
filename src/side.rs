use serde::{Deserialize, Serialize};

/// The side of an order or price level, shared by DTOs and application code.
/// Buy means bid; Sell means ask. The discriminants are SBE wire values and
/// must not be renumbered.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Buy => "Buy",
            Self::Sell => "Sell",
        })
    }
}

impl TryFrom<u8> for Side {
    type Error = InvalidSide;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Buy),
            1 => Ok(Self::Sell),
            _ => Err(InvalidSide(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSide(pub u8);

impl std::fmt::Display for InvalidSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid side {}; expected 0 (Buy) or 1 (Sell)", self.0)
    }
}

impl std::error::Error for InvalidSide {}
