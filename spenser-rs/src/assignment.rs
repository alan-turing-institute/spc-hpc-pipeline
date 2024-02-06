use crate::config::{Age, Config, Year};
use polars::prelude::*;
use rand::{rngs::StdRng, SeedableRng};
use std::{collections::HashMap, path::PathBuf};

const ADULT_AGE: Age = Age(16);

#[derive(Debug)]
struct Assignment {
    region: String,
    year: Year,
    output_dir: PathBuf,
    scotland: bool,
    h_data: DataFrame,
    p_data: DataFrame,
    strictmode: bool,
    geog_lookup: DataFrame,
    hrp_dist: HashMap<String, DataFrame>,
    hrp_index: HashMap<String, Vec<usize>>,
    partner_hrp_dist: DataFrame,
    child_hrp_dist: DataFrame,
    rng: StdRng,
}

fn read_geog_lookup(path: &str) -> anyhow::Result<DataFrame> {
    let mut df = CsvReader::from_path(path)?.finish()?;
    df.rename("OA", "oa")?
        .rename("MSOA", "msoa")?
        .rename("LAD", "la")?
        .rename("LSOA", "lsoa")?;
    Ok(df)
}

// TODO: refine paths
const PERSISTENT_DATA: &str = "data/microsimulation/persistent_data/";

impl Assignment {
    pub fn new(region: &str, config: &Config) -> anyhow::Result<Assignment> {
        let h_file = config
            .data_dir
            .join(format!(
                "ssm_hh_{}_{}_{}.csv",
                region, config.household_resolution, config.year
            ))
            .to_path_buf();

        let p_file = config
            .data_dir
            .join(format!(
                "ssm_{}_{}_{}_{}.csv",
                region, config.person_resolution, config.projection, config.year
            ))
            .to_path_buf();

        let geog_lookup = read_geog_lookup(&format!("{PERSISTENT_DATA}/gb_geog_lookup.csv.gz"))?;
        let mut hrp_dist: HashMap<String, DataFrame> = HashMap::new();
        hrp_dist.insert(
            "sgl".to_string(),
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/hrp_sgl_dist.csv"))?.finish()?,
        );
        hrp_dist.insert(
            "cpl".to_string(),
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/hrp_cpl_dist.csv"))?.finish()?,
        );
        hrp_dist.insert(
            "sp".to_string(),
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/hrp_sp_dist.csv"))?.finish()?,
        );
        hrp_dist.insert(
            "mix".to_string(),
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/hrp_dist.csv"))?.finish()?,
        );

        let mut hrp_index: HashMap<String, Vec<usize>> = HashMap::new();
        hrp_index.insert("sgl".to_string(), vec![1]);
        hrp_index.insert("cpl".to_string(), vec![2, 3]);
        hrp_index.insert("sp".to_string(), vec![4]);
        hrp_index.insert("mix".to_string(), vec![5]);

        // # distribution of partner age/sex/eth by HRP age/sex/eth
        // self.partner_hrp_dist = pd.read_csv("./persistent_data/partner_hrp_dist.csv")

        let partner_hrp_dist =
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/partner_hrp_dist.csv"))?.finish()?;
        // # distribution of child age/sex/eth by HRP age/sex/eth
        // self.child_hrp_dist = pd.read_csv("./persistent_data/child_hrp_dist.csv")
        let child_hrp_dist =
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/child_hrp_dist.csv"))?.finish()?;

        Ok(Self {
            region: region.to_owned(),
            year: config.year.to_owned(),
            output_dir: config.data_dir.to_owned(),
            scotland: region.starts_with('S'),
            h_data: CsvReader::from_path(h_file.as_os_str())?.finish()?,
            p_data: CsvReader::from_path(p_file.as_os_str())?.finish()?,
            strictmode: config.strict,
            geog_lookup,
            hrp_dist,
            hrp_index,
            partner_hrp_dist,
            child_hrp_dist,
            rng: StdRng::seed_from_u64(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::config::{self, Resolution};

    use super::*;

    #[test]
    fn test_read_geog_lookup() -> anyhow::Result<()> {
        let df = read_geog_lookup(&format!("{PERSISTENT_DATA}/gb_geog_lookup.csv.gz"))?;
        println!("{}", df);
        Ok(())
    }

    #[test]
    fn test_assignment() -> anyhow::Result<()> {
        let config = Config {
            person_resolution: Resolution::MSOA11,
            household_resolution: Resolution::OA11,
            projection: config::Projection::PPP,
            strict: false,
            year: Year(2020),
            data_dir: PathBuf::from_str("data/microsimulation/data")?,
            profile: false,
        };
        let assignment = Assignment::new("E06000001", &config)?;
        println!("{:?}", assignment);
        Ok(())
    }
}
