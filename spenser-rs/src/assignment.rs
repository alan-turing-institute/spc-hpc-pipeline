use crate::config::{Age, Config, Year};
use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use std::{collections::HashMap, path::PathBuf};
use typed_index_collections::TiVec;
const ADULT_AGE: Age = Age(16);

#[derive(Debug)]
struct Assignment {
    region: String,
    year: Year,
    output_dir: PathBuf,
    scotland: bool,
    // h_data: TiVec<Household>,
    // p_data: TiVec<Person>,
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

// See example: https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.apply
fn replace_i32(mapping: &HashMap<i32, i32>) -> impl (Fn(&Series) -> Series) + '_ {
    |series: &Series| -> Series {
        series
            .cast(&DataType::Int32)
            .unwrap()
            .i32()
            .unwrap()
            .into_iter()
            .map(|opt_el: Option<i32>| opt_el.map(|el| *mapping.get(&el).unwrap_or(&el)))
            .collect::<Int32Chunked>()
            .into_series()
    }
}

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
        let partner_hrp_dist =
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/partner_hrp_dist.csv"))?.finish()?;

        // # distribution of child age/sex/eth by HRP age/sex/eth
        let child_hrp_dist =
            CsvReader::from_path(&format!("{PERSISTENT_DATA}/child_hrp_dist.csv"))?.finish()?;

        let scotland = region.starts_with('S');
        let mut h_data = CsvReader::from_path(h_file.as_os_str())?.finish()?;
        let mut p_data = CsvReader::from_path(p_file.as_os_str())?.finish()?;
        // TODO: check the mapping
        if !scotland {
            let eth_mapping = HashMap::from([
                (-1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 4),
                (7, 5),
                (8, 5),
                (9, 5),
                (10, 5),
                (12, 6),
                (13, 6),
                (14, 6),
                (15, 6),
                (16, 6),
                (18, 7),
                (19, 7),
                (20, 7),
                (22, 8),
                (23, 8),
            ]);

            p_data.apply("DC2101EW_C_ETHPUK11", replace_i32(&eth_mapping))?;
        } else {
            // TODO: check the mapping
            let eth_remapping =
                HashMap::from([(-1, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 8)]);
            p_data.apply("DC2101EW_C_ETHPUK11", replace_i32(&eth_remapping))?;
            h_data.apply("LC4202_C_ETHHUK11", replace_i32(&eth_remapping))?;
        }

        Ok(Self {
            region: region.to_owned(),
            year: config.year.to_owned(),
            output_dir: config.data_dir.to_owned(),
            scotland,
            h_data,
            p_data,
            strictmode: config.strict,
            geog_lookup,
            hrp_dist,
            hrp_index,
            partner_hrp_dist,
            child_hrp_dist,
            rng: StdRng::seed_from_u64(0),
        })
    }

    fn sample_hrp(&self, msoa: &str, oas: &[String]) -> anyhow::Result<()> {
        // let h_ref = self.h_data.filter(col("Area"))

        // TODO: fix types
        let hh_type = "sgl".to_string();

        // let sample = self
        //     .hrp_dist
        //     .get(&hh_type)
        //     .unwrap()
        //     .select(col("n"))?
        //     .iter();
        // .sample_n_literal(10, true, true, Some(0))?;

        todo!()
    }

    pub fn run(&self) {
        todo!()
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
