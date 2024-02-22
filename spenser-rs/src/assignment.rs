// TODO: to remove
#![allow(unused_imports)]
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use hashbrown::HashSet;
use polars::prelude::*;
use rand::distributions::{Distribution, WeightedIndex};
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use tracing::info;
use typed_index_collections::TiVec;

use crate::{
    config::{Config, Year},
    person::ChildHRPerson,
    queues::AdultOrChild,
    return_some, OA,
};
use crate::{
    household::{Household, HID},
    queues::Queues,
};
use crate::{
    person::{HRPerson, PartnerHRPerson, Person, HRPID, PID},
    MSOA,
};
use crate::{Age, Eth, Sex};

// TODO: remove, temporary helper for debugging.
fn input() {
    std::io::stdin().read_exact(&mut [0]).unwrap();
}

#[derive(Debug)]
struct Assignment {
    pub region: String,
    pub year: Year,
    pub output_dir: PathBuf,
    pub scotland: bool,
    pub h_data: TiVec<HID, Household>,
    pub p_data: TiVec<PID, Person>,
    pub strictmode: bool,
    pub geog_lookup: DataFrame,
    pub hrp_dist: BTreeMap<String, TiVec<HRPID, HRPerson>>,
    pub hrp_index: BTreeMap<String, Vec<i32>>,
    pub partner_hrp_dist: TiVec<HRPID, PartnerHRPerson>,
    pub child_hrp_dist: TiVec<HRPID, ChildHRPerson>,
    pub rng: StdRng,
    pub fail: usize,
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

fn read_csv<P: AsRef<Path>, K, V: for<'a> Deserialize<'a>>(path: P) -> anyhow::Result<TiVec<K, V>> {
    Ok(csv::Reader::from_path(path)?
        .deserialize()
        .collect::<Result<TiVec<K, V>, _>>()?)
}

trait GetSetEth {
    fn get_eth(&self) -> &Eth;
    fn set_eth(&mut self, eth: Eth);
}

impl GetSetEth for Person {
    fn get_eth(&self) -> &Eth {
        &self.eth
    }
    fn set_eth(&mut self, eth: Eth) {
        self.eth = eth;
    }
}

impl GetSetEth for Household {
    fn get_eth(&self) -> &Eth {
        &self.lc4202_c_ethhuk11
    }

    fn set_eth(&mut self, eth: Eth) {
        self.lc4202_c_ethhuk11 = eth;
    }
}

fn map_eth<K, V: GetSetEth>(
    data: TiVec<K, V>,
    eth_mapping: &HashMap<Eth, Eth>,
) -> anyhow::Result<TiVec<K, V>> {
    data.into_iter()
        .map(|mut person| {
            match eth_mapping
                // TODO: fix int types
                .get(person.get_eth())
                .cloned()
                .ok_or(anyhow!("Invalid mapping."))
            {
                Ok(new_val) => {
                    person.set_eth(new_val);
                    Ok(person)
                }
                Err(e) => Err(e),
            }
        })
        .collect::<anyhow::Result<TiVec<K, V>>>()
}

enum Parent {
    Single,
    Couple,
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
        let mut hrp_dist: BTreeMap<String, TiVec<HRPID, HRPerson>> = BTreeMap::new();
        hrp_dist.insert(
            "sgl".to_string(),
            read_csv(format!("{PERSISTENT_DATA}/hrp_sgl_dist.csv"))?,
        );
        hrp_dist.insert(
            "cpl".to_string(),
            read_csv(format!("{PERSISTENT_DATA}/hrp_cpl_dist.csv"))?,
        );
        hrp_dist.insert(
            "sp".to_string(),
            read_csv(format!("{PERSISTENT_DATA}/hrp_sp_dist.csv"))?,
        );
        hrp_dist.insert(
            "mix".to_string(),
            read_csv(format!("{PERSISTENT_DATA}/hrp_dist.csv"))?,
        );

        let mut hrp_index: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        hrp_index.insert("sgl".to_string(), vec![1]);
        hrp_index.insert("cpl".to_string(), vec![2, 3]);
        hrp_index.insert("sp".to_string(), vec![4]);
        hrp_index.insert("mix".to_string(), vec![5]);

        // # distribution of partner age/sex/eth by HRP age/sex/eth
        let partner_hrp_dist = read_csv(format!("{PERSISTENT_DATA}/partner_hrp_dist.csv"))?;

        // # distribution of child age/sex/eth by HRP age/sex/eth
        let child_hrp_dist = read_csv(format!("{PERSISTENT_DATA}/child_hrp_dist.csv"))?;

        let scotland = region.starts_with('S');
        let mut h_data: TiVec<HID, Household> = read_csv(h_file)?;
        let mut p_data: TiVec<PID, Person> = read_csv(p_file)?;

        // TODO: check the mapping
        if !scotland {
            let eth_mapping = [
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
            ]
            .into_iter()
            .map(|(x, y)| (Eth(x), Eth(y)))
            .collect::<HashMap<Eth, Eth>>();
            p_data = map_eth(p_data, &eth_mapping)?;
        } else {
            // TODO: check the mapping
            let eth_remapping = [(-1, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 8)]
                .into_iter()
                .map(|(x, y)| (Eth(x), Eth(y)))
                .collect::<HashMap<Eth, Eth>>();
            p_data = map_eth(p_data, &eth_remapping)?;
            h_data = map_eth(h_data, &eth_remapping)?;
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
            fail: 0,
        })
    }

    fn sample_hrp(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        for hh_type in ["sgl", "cpl", "sp", "mix"]
            .into_iter()
            .map(|s| s.to_owned())
        {
            let hrp_dist = self.hrp_dist.get(&hh_type).unwrap();
            let weighted_idx = WeightedIndex::new(hrp_dist.iter().map(|hrp| hrp.n)).unwrap();
            let idxs = self.hrp_index.get(&hh_type).unwrap();
            let h_ref: Vec<_> = self
                .h_data
                .iter_mut()
                .filter(|household| {
                    idxs.contains(&household.lc4408_c_ahthuk11)
                        && oas.contains(&household.oa)
                        && household.hrpid.is_none()
                })
                .collect();

            // Get sample of HRPs
            let sample = (0..h_ref.len()).fold(Vec::new(), |mut acc, _| {
                let hrpid = HRPID(weighted_idx.sample(&mut self.rng));
                let el = hrp_dist.get(hrpid).unwrap();
                acc.push((hrpid.to_owned(), el));
                acc
            });

            // Loop over sample HRPs and match PIDs
            for ((_, sample_person), household) in sample.iter().zip(h_ref) {
                // Demographics
                let age = sample_person.age;
                let sex = sample_person.sex;
                let eth = sample_person.eth;

                // Try exact match over unmatched
                if let Some(pid) =
                    queues.sample_person(msoa, age, sex, eth, AdultOrChild::Adult, &self.p_data)
                {
                    // Assign pid to household
                    household.hrpid = Some(pid);
                    // Assign household to person
                    self.p_data
                        .get_mut(pid)
                        .unwrap_or_else(|| panic!("Invalid {pid}"))
                        .hid = Some(HID(household.hid.to_owned() as usize));

                    // If single person household, filled
                    if household.lc4408_c_ahthuk11 == 1 {
                        household.filled = Some(true)
                    }
                    println!(
                        "Assigned: {pid:9}, unmatched: {:6}, matched: {:6}, failed: {:6}",
                        queues.unmatched.len(),
                        queues.matched.len(),
                        self.fail
                    );
                } else {
                    return Err(anyhow!("No match for: {sample_person:?}").context("sample_hrp()"));
                }
            }
        }
        Ok(())
    }

    fn sample_partner(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        let h2_ref: Vec<_> = self
            .h_data
            .iter_mut()
            .filter(|household| {
                [2, 3].contains(&household.lc4408_c_ahthuk11)
                    && oas.contains(&household.oa)
                    && household.filled != Some(true)
            })
            .collect();

        // Sampling by age and ethnicity
        let mut dist_by_ae = HashMap::new();
        // Sampling by age
        let mut dist_by_a = HashMap::new();
        // Populate lookups
        self.partner_hrp_dist
            .iter_enumerated()
            .for_each(|(hrpid, partner)| {
                dist_by_ae
                    .entry((partner.agehrp, partner.eth))
                    .and_modify(|v: &mut Vec<(HRPID, usize)>| {
                        v.push((hrpid, partner.n));
                    })
                    .or_insert(vec![(hrpid, partner.n)]);
                dist_by_a
                    .entry(partner.agehrp)
                    .and_modify(|v: &mut Vec<(HRPID, usize)>| {
                        v.push((hrpid, partner.n));
                    })
                    .or_insert(vec![(hrpid, partner.n)]);
            });
        // Construct vec off HRPID and weights
        let dist = self
            .partner_hrp_dist
            .iter_enumerated()
            .map(|(idx, person)| (idx.to_owned(), person.n))
            .collect();
        for household in h2_ref {
            let hrpid = household.hrpid.expect("Household is not assigned a PID");
            let hrp = self.p_data.get(hrpid).expect("Invalid HRPID");
            let hrp_sex = hrp.sex;
            let hrp_age = hrp.age;
            let hrp_eth = hrp.eth;

            // Pick dist
            let dist = dist_by_ae.get(&(hrp_age, hrp_eth)).unwrap_or_else(|| {
                println!(
                    "Partner-HRP not sampled: {:3}, {:3?}, {:3?} - resample withouth eht.",
                    hrp_age, hrp_sex, hrp_eth
                );
                dist_by_a.get(&hrp_age).unwrap_or_else(|| {
                    println!(
                        "Partner-HRP not sampled: {}, {:?}, {:?}",
                        hrp_age, hrp_sex, hrp_eth
                    );
                    &dist
                })
            });

            let partner_sample_id = dist.choose_weighted(&mut self.rng, |item| item.1)?.0;
            let partner_sample = self
                .partner_hrp_dist
                .get(partner_sample_id)
                .ok_or(anyhow!("Invalid HRPID: {partner_sample_id}"))?;
            let age = partner_sample.age;
            let sex = if partner_sample.samesex {
                hrp_sex
            } else {
                hrp_sex.opposite()
            };

            // TODO: check is "ethnicityew"
            let eth: Eth = partner_sample.ethnicityew.into();

            // Sample a HR person
            if let Some(pid) =
                queues.sample_person(msoa, age, sex, eth, AdultOrChild::Adult, &self.p_data)
            {
                // Assign pid to household
                household.hrpid = Some(pid);
                // Assign household to person
                self.p_data
                    .get_mut(pid)
                    .unwrap_or_else(|| panic!("Invalid {pid}"))
                    // TODO: fix the household HID field to have HID type
                    .hid = Some(HID(household.hid.to_owned() as usize));

                // If single person household, filled
                if household.lc4404_c_sizhuk11 == 2 {
                    household.filled = Some(true)
                }
                println!(
                    "Assigned: {pid:9}, unmatched: {:6}, matched: {:6}, failed: {:6}",
                    queues.unmatched.len(),
                    queues.matched.len(),
                    self.fail
                );
            } else {
                println!("No partner match!");
                self.fail += 1;
            }
        }

        Ok(())
    }

    fn sample_child(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        num_occ: i32,
        mark_filled: bool,
        parent: Parent,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        let hsp_ref: Vec<_> = self
            .h_data
            .iter_mut()
            .filter(|household| {
                // TODO: check condition as for other sample
                oas.contains(&household.oa)
                    && household.lc4404_c_sizhuk11.eq(&num_occ)
                    && match parent {
                        Parent::Single => household.lc4408_c_ahthuk11.eq(&4),
                        Parent::Couple => [2, 3].contains(&household.lc4408_c_ahthuk11),
                    }
                    && household.filled != Some(true)
            })
            .collect();

        // Sampling by age and ethnicity
        let mut dist_by_ae = HashMap::new();
        // Sampling by age
        let mut dist_by_a = HashMap::new();
        // Sampling by eth
        let mut dist_by_e = HashMap::new();

        // Populate lookups
        self.child_hrp_dist
            .iter_enumerated()
            // TODO: check the conditions with the dataframe filter
            .for_each(|(hrpid, child)| {
                dist_by_ae
                    .entry((child.agehrp, child.eth))
                    .and_modify(|v: &mut Vec<(HRPID, usize)>| {
                        v.push((hrpid, child.n));
                    })
                    .or_insert(vec![(hrpid, child.n)]);
                dist_by_a
                    .entry(child.agehrp)
                    .and_modify(|v: &mut Vec<(HRPID, usize)>| {
                        v.push((hrpid, child.n));
                    })
                    .or_insert(vec![(hrpid, child.n)]);
                dist_by_e
                    .entry(child.eth)
                    .and_modify(|v: &mut Vec<(HRPID, usize)>| {
                        v.push((hrpid, child.n));
                    })
                    .or_insert(vec![(hrpid, child.n)]);
            });

        // Construct vec from HRPID and weights
        let dist = self
            .child_hrp_dist
            .iter_enumerated()
            .map(|(idx, person)| (idx.to_owned(), person.n))
            .collect();

        for household in hsp_ref {
            let hrpid = household.hrpid.expect("HRPID is not assigned.");
            let hrp_person = self.p_data.get(hrpid).expect("Invalid PID.");
            let hrp_age = hrp_person.age;
            // TODO: check why unused
            let hrp_sex = hrp_person.sex;
            let hrp_eth = hrp_person.eth;

            // Pick dist
            let dist = match parent {
                Parent::Single => dist_by_ae.get(&(hrp_age, hrp_eth)).unwrap_or_else(|| {
                    dist_by_a
                        .get(&hrp_age)
                        .unwrap_or_else(|| dist_by_e.get(&hrp_eth).unwrap_or(&dist))
                }),
                Parent::Couple => dist_by_ae
                    .get(&(hrp_age, hrp_eth))
                    .unwrap_or_else(|| panic!("No matching {hrp_age}, {hrp_eth} in distribution.")),
            };
            // Sample:: TODO: make sep fn
            let child_sample_id = dist.choose_weighted(&mut self.rng, |item| item.1)?.0;
            let child_sample = self
                .child_hrp_dist
                .get(child_sample_id)
                .ok_or(anyhow!("Invalid HRPID: {child_sample_id}"))?;
            let age = child_sample.age;
            let sex = child_sample.sex;
            let eth = child_sample.eth;

            // Get match from population
            if let Some(pid) =
                queues.sample_person(msoa, age, sex, eth, AdultOrChild::Child, &self.p_data)
            {
                println!("{}", pid);
                self.p_data
                    .get_mut(pid)
                    .unwrap_or_else(|| panic!("Invalid {pid}"))
                    // TODO: fix the household HID field to have HID type
                    .hid = Some(HID(household.hid.to_owned() as usize));
                if mark_filled {
                    household.filled = Some(true)
                }
                println!(
                    "Assigned: {pid:9}, unmatched: {:6}, matched: {:6}, failed: {:6}",
                    queues.unmatched.len(),
                    queues.matched.len(),
                    self.fail
                );
            } else {
                println!(
                    "child not found,  age: {}, sex: {:?}, eth: {:?}",
                    age, sex, eth
                );
            }
        }

        Ok(())
    }

    fn fill_multi(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        nocc: usize,
        mark_filled: bool,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        todo!()
    }
    fn fill_communal(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        todo!()
    }
    fn assign_surplus_adults(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn assign_surplus_children(
        &mut self,
        msoa: &MSOA,
        oas: &HashSet<OA>,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    // TODO: add type for LAD
    pub fn run(&mut self) -> anyhow::Result<()> {
        // Create queues
        let mut queues = Queues::new(&self.p_data, &mut self.rng);

        // Deterministic ordering
        let msoas: BTreeSet<MSOA> = self
            .p_data
            .iter()
            .map(|person| person.msoa.to_owned())
            .collect();

        for msoa in msoas.iter() {
            let oas = self
                .geog_lookup
                .clone()
                .lazy()
                .filter(col("msoa").eq(lit(String::from(msoa.to_owned()))))
                .select([col("oa")])
                .collect()?;
            let oas: HashSet<OA> = oas
                .iter()
                .next()
                .unwrap()
                .str()?
                .into_iter()
                .map(|el| el.unwrap().to_owned().into())
                .collect();
            println!("{:?}", msoa);
            println!("{:?}", oas);

            // Sample HRP
            println!("> assigning HRPs");
            self.sample_hrp(msoa, &oas, &mut queues)?;
            // self.stats();

            // Sample partner
            // TODO: check all partners assigned (from python)
            println!("> assigning partners to HRPs where appropriate");
            self.sample_partner(msoa, &oas, &mut queues)?;
            // self.stats()

            println!("> assigning child 1 to single-parent households");
            // // self.__sample_single_parent_child(msoa, oas, 2, mark_filled=True)
            self.sample_child(msoa, &oas, 2, true, Parent::Single, &mut queues)?;
            // // self.stats()

            println!("> assigning child 2 to single-parent households");
            // // self.__sample_single_parent_child(msoa, oas, 3, mark_filled=True)
            self.sample_child(msoa, &oas, 3, true, Parent::Single, &mut queues)?;
            // // self.stats()

            println!("> assigning child 3 to single-parent households");
            // // self.__sample_single_parent_child(msoa, oas, 4, mark_filled=False)
            self.sample_child(msoa, &oas, 4, true, Parent::Single, &mut queues)?;
            // // self.stats()

            // // # TODO if partner hasnt been assigned then household may be incorrectly marked filled
            println!("assigning child 1 to couple households");
            // // self.__sample_couple_child(msoa, oas, 3, mark_filled=True)
            self.sample_child(msoa, &oas, 3, true, Parent::Couple, &mut queues)?;
            // // self.stats()

            // // # TODO if partner hasnt been assigned then household may be incorrectly marked filled
            println!("assigning child 2 to single-parent households");
            // // self.__sample_couple_child(msoa, oas, 4, mark_filled=False)
            self.sample_child(msoa, &oas, 4, true, Parent::Couple, &mut queues)?;
            // // self.stats()

            // println!("multi-person households");
            // // self.__fill_multi(msoa, oas, 2)
            // self.fill_multi(msoa, &oas, 2, true, &mut queues)?;
            // // self.__fill_multi(msoa, oas, 3)
            // self.fill_multi(msoa, &oas, 3, true, &mut queues)?;
            // // self.__fill_multi(msoa, oas, 4, mark_filled=False)
            // self.fill_multi(msoa, &oas, 4, false, &mut queues)?;
            // // self.stats()

            // println!("assigning people to communal establishments");
            // // self.__fill_communal(msoa, oas)
            // self.fill_communal(msoa, &oas, &mut queues)?;
            // // self.stats()

            // println!("assigning surplus adults");
            // self.assign_surplus_adults(msoa, &oas, &mut queues)?;
            // // self.stats()

            // println!("assigning surplus children");
            // self.assign_surplus_children(msoa, &oas, &mut queues)?;
            // // self.stats()
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, str::FromStr};

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

    #[test]
    fn test_run() -> anyhow::Result<()> {
        let config = Config {
            person_resolution: Resolution::MSOA11,
            household_resolution: Resolution::OA11,
            projection: config::Projection::PPP,
            strict: false,
            year: Year(2020),
            data_dir: PathBuf::from_str("data/microsimulation/data")?,
            profile: false,
        };
        let mut assignment = Assignment::new("E06000001", &config)?;
        assignment.run()?;
        Ok(())
    }
}
