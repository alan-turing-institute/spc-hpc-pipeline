use serde::{Deserialize, Serialize};

pub mod assignment;
pub mod config;
pub mod household;
pub mod person;
pub(crate) mod queues;

macro_rules! return_some {
    ($arg:expr) => {
        if $arg.is_some() {
            return $arg;
        }
    };
}
pub(crate) use return_some;

const ADULT_AGE: Age = Age(16);

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

impl Sex {
    fn opposite(&self) -> Self {
        Self(3 - self.0)
    }
}

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

impl From<EthEW> for Eth {
    fn from(value: EthEW) -> Self {
        Self(value.0)
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
