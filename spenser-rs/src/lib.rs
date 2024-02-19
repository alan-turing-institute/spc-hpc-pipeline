use serde::{Deserialize, Serialize};

pub mod assignment;
pub mod config;
pub mod household;
pub mod person;

#[derive(Hash, Clone, Debug, Serialize, Deserialize)]
struct MSOA(String);

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sex(pub usize);

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Age(pub usize);

#[derive(Hash, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Eth(pub i32);

impl From<i32> for Eth {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for Age {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
