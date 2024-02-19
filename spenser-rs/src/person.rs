use crate::{household::HID, Age, Eth, Sex};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct PID(pub usize);
impl std::fmt::Display for PID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PID #{}", self.0)
    }
}
impl From<usize> for PID {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl From<PID> for usize {
    fn from(value: PID) -> Self {
        value.0
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Person {
    #[serde(rename = "PID")]
    pub pid: usize,
    #[serde(rename = "Area")]
    pub area: String,
    #[serde(rename = "DC1117EW_C_SEX")]
    pub sex: Sex,
    #[serde(rename = "DC1117EW_C_AGE")]
    pub age: Age,
    #[serde(rename = "DC2101EW_C_ETHPUK11")]
    pub eth: Eth,
    #[serde(rename = "HID")]
    pub hid: Option<HID>,
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct HRPID(pub usize);
impl std::fmt::Display for HRPID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HRPID #{}", self.0)
    }
}
impl From<usize> for HRPID {
    fn from(value: usize) -> Self {
        Self(value)
    }
}
impl From<HRPID> for usize {
    fn from(value: HRPID) -> Self {
        value.0
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct HRPerson {
    #[serde(rename = "age")]
    pub age: Age,
    #[serde(rename = "sex")]
    pub sex: Option<Sex>,
    #[serde(rename = "ethhuk11")]
    pub eth: Eth,
    pub n: usize,
}
// age,agehrp,ethnicityew,ethhuk11,n,samesex
// 17,17,2,2,1,FALSE

// #[derive(Serialize, Deserialize, Debug, Hash)]
// pub struct PartnerHRPerson {
//     #[serde(rename = "age")]
//     pub age: usize,
//     #[serde(rename = "age")]
//     pub age: usize,
//     #[serde(rename = "ethhuk11")]
//     pub eth: usize,
//     pub n: usize,
// }

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_deserialize_person() -> anyhow::Result<()> {
        let test_csv = std::fs::read_to_string("data/tests/test_person.csv")?;
        let mut rdr = csv::Reader::from_reader(test_csv.as_bytes());
        for result in rdr.deserialize() {
            let record: Person = result?;
            println!("{:?}", record);
        }
        Ok(())
    }
    #[test]
    fn test_deserialize_hrp() -> anyhow::Result<()> {
        let test_csv = std::fs::read_to_string("data/tests/test_hrp_sgl.csv")?;
        let mut rdr = csv::Reader::from_reader(test_csv.as_bytes());
        for result in rdr.deserialize() {
            let record: HRPerson = result?;
            println!("{:?}", record);
        }
        Ok(())
    }
}
