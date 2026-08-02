use std::fmt;
use std::str::FromStr;

/// Enum to represent boolean property values in string form (0/1)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolPropertyValue {
    /// Represents false (0)
    Off,
    /// Represents true (1)
    On,
}

/// Error type for BoolPropertyValue parsing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseBoolPropertyValueError {
    pub invalid_value: String,
}

impl fmt::Display for ParseBoolPropertyValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid property value: {}", self.invalid_value)
    }
}

impl std::error::Error for ParseBoolPropertyValueError {}

impl FromStr for BoolPropertyValue {
    type Err = ParseBoolPropertyValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "0" => Ok(BoolPropertyValue::Off),
            "1" => Ok(BoolPropertyValue::On),
            _ => Err(ParseBoolPropertyValueError {
                invalid_value: value.to_string(),
            }),
        }
    }
}

impl BoolPropertyValue {
    /// Convert BoolPropertyValue to a boolean
    pub fn to_bool(self) -> bool {
        match self {
            BoolPropertyValue::Off => false,
            BoolPropertyValue::On => true,
        }
    }
}

impl fmt::Display for BoolPropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoolPropertyValue::Off => write!(f, "0"),
            BoolPropertyValue::On => write!(f, "1"),
        }
    }
}
