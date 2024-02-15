use serde::{Deserialize, Serialize};

#[derive(Debug)]
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

#[derive(Serialize, Deserialize, Debug, Hash)]
pub struct Person {
    #[serde(rename = "PID")]
    pub pid: usize,
    #[serde(rename = "Area")]
    pub area: String,
    #[serde(rename = "DC1117EW_C_SEX")]
    pub sex: usize,
    #[serde(rename = "DC1117EW_C_AGE")]
    pub age: usize,
    #[serde(rename = "DC2101EW_C_ETHPUK11")]
    pub eth: i32,
}

#[derive(Debug)]
pub struct HRPID(pub usize);
impl std::fmt::Display for HRPID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HRPID #{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Hash)]
pub struct HRPerson {
    #[serde(rename = "age")]
    pub age: usize,
    #[serde(rename = "sex")]
    pub sex: usize,
    #[serde(rename = "ethhuk11")]
    pub eth: usize,
    pub n: usize,
}

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
