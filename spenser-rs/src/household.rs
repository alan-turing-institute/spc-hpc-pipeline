use serde::{Deserialize, Serialize};

use crate::{
    person::{HRPID, PID},
    Eth,
};

type MSOA = String;
type UInt = i32;
type Int = i32;
#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct HID(pub usize);
impl std::fmt::Display for HID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HID #{}", self.0)
    }
}
impl From<usize> for HID {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Household {
    #[serde(rename = "HID")]
    pub hid: Int,
    #[serde(rename = "Area")]
    // TODO: should be OA? Use import of wrapper types instead.
    pub area: MSOA,
    #[serde(rename = "LC4402_C_TYPACCOM")]
    pub lc4402_c_typaccom: UInt,
    #[serde(rename = "QS420_CELL")]
    pub qs420_cell: Int,
    #[serde(rename = "LC4402_C_TENHUK11")]
    pub lc4402_c_tenhuk11: UInt,
    #[serde(rename = "LC4408_C_AHTHUK11")]
    pub lc4408_c_ahthuk11: Int,
    #[serde(rename = "CommunalSize")]
    pub communal_size: Int,
    #[serde(rename = "LC4404_C_SIZHUK11")]
    pub lc4404_c_sizhuk11: UInt,
    #[serde(rename = "LC4404_C_ROOMS")]
    pub lc4404_c_rooms: UInt,
    #[serde(rename = "LC4405EW_C_BEDROOMS")]
    pub lc4405ew_c_bedrooms: UInt,
    #[serde(rename = "LC4408EW_C_PPBROOMHEW11")]
    pub lc4408ew_c_ppbroomhew11: Int,
    #[serde(rename = "LC4402_C_CENHEATHUK11")]
    pub lc4402_c_cenheathuk11: UInt,
    #[serde(rename = "LC4605_C_NSSEC")]
    pub lc4605_c_nssec: UInt,
    #[serde(rename = "LC4202_C_ETHHUK11")]
    pub lc4202_c_ethhuk11: Eth,
    #[serde(rename = "LC4202_C_CARSNO")]
    pub lc4202_c_carsno: UInt,
    #[serde(rename = "HRPID")]
    pub hrpid: Option<PID>,
    #[serde(rename = "FILLED")]
    pub filled: Option<bool>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_deserialize_hh() -> anyhow::Result<()> {
        let test_csv = std::fs::read_to_string("data/tests/test_hh.csv")?;
        let mut rdr = csv::Reader::from_reader(test_csv.as_bytes());
        for result in rdr.deserialize() {
            let record: Household = result?;
            println!("{:?}", record);
        }
        Ok(())
    }
}
