use serde::{Deserialize, Serialize};

pub mod assignment;
pub mod config;
pub mod household;
pub mod person;

// TODO: use type instead of string in assignment
#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct MSOA(String);

impl From<&str> for MSOA {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}
impl From<String> for MSOA {
    fn from(value: String) -> Self {
        Self(value)
    }
}

// TODO: use type instead of string in assignment
#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct OA(String);

impl From<&str> for OA {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}
impl From<String> for OA {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sex(pub usize);

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Age(pub usize);

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Eth(pub i32);

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EthEW(pub i32);

impl From<i32> for Eth {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<i32> for EthEW {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for Age {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
