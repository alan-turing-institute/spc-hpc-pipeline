use serde::{Deserialize, Serialize};

type MSOA = String;
type UInt = i32;
type Int = i32;

#[derive(Serialize, Deserialize, Debug)]
struct Household {
    #[serde(rename = "HID")]
    hid: Int,
    #[serde(rename = "Area")]
    area: MSOA,
    #[serde(rename = "LC4402_C_TYPACCOM")]
    lc4402_c_typaccom: UInt,
    #[serde(rename = "QS420_CELL")]
    qs420_cell: Int,
    #[serde(rename = "LC4402_C_TENHUK11")]
    lc4402_c_tenhuk11: UInt,
    #[serde(rename = "LC4408_C_AHTHUK11")]
    lc4408_c_ahthuk11: Int,
    #[serde(rename = "CommunalSize")]
    communal_size: Int,
    #[serde(rename = "LC4404_C_SIZHUK11")]
    lc4404_c_sizhuk11: UInt,
    #[serde(rename = "LC4404_C_ROOMS")]
    lc4404_c_rooms: UInt,
    #[serde(rename = "LC4405EW_C_BEDROOMS")]
    lc4405ew_c_bedrooms: UInt,
    #[serde(rename = "LC4408EW_C_PPBROOMHEW11")]
    lc4408ew_c_ppbroomhew11: Int,
    #[serde(rename = "LC4402_C_CENHEATHUK11")]
    lc4402_c_cenheathuk11: UInt,
    #[serde(rename = "LC4605_C_NSSEC")]
    lc4605_c_nssec: UInt,
    #[serde(rename = "LC4202_C_ETHHUK11")]
    lc4202_c_ethhuk11: UInt,
    #[serde(rename = "LC4202_C_CARSNO")]
    lc4202_c_carsno: UInt,
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
