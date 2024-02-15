use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Person {
    #[serde(rename = "PID")]
    pid: usize,
    #[serde(rename = "Area")]
    area: String,
    #[serde(rename = "DC1117EW_C_SEX")]
    sex: usize,
    #[serde(rename = "DC1117EW_C_AGE")]
    age: usize,
    #[serde(rename = "DC2101EW_C_ETHPUK11")]
    eth: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct HRPerson {
    #[serde(rename = "age")]
    age: usize,
    #[serde(rename = "sex")]
    sex: usize,
    #[serde(rename = "ethhuk11")]
    eth: usize,
    n: usize,
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
