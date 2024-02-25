use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub enum Resolution {
    MSOA11,
    OA11,
}

impl std::fmt::Display for Resolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Resolution::MSOA11 => write!(f, "MSOA11"),
            Resolution::OA11 => write!(f, "OA11"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Projection {
    #[serde(rename = "ppp")]
    PPP,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Projection::PPP => write!(f, "ppp"),
        }
    }
}

pub type YearInt = u32;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Year(pub YearInt);

impl std::fmt::Display for Year {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub person_resolution: Resolution,
    pub household_resolution: Resolution,
    pub projection: Projection,
    pub strict: bool,
    pub year: Year,
    pub data_dir: PathBuf,
    pub profile: bool,
}
