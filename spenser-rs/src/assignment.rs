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
use typed_index_collections::TiVec;

use crate::household::{Household, HID};
use crate::person::{HRPerson, PartnerHRPerson, Person, HRPID, PID};
use crate::{
    config::{Config, Year},
    return_some,
};
use crate::{Age, Eth, Sex};

const ADULT_AGE: Age = Age(16);

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
    pub child_hrp_dist: TiVec<HRPID, HRPerson>,
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

fn update_pid_vec(
    v: &mut Vec<PID>,
    matched: &mut HashSet<PID>,
    unmatched: &mut HashSet<PID>,
) -> Option<PID> {
    while let Some(el) = v.pop() {
        if matched.contains(&el) {
            continue;
        }
        unmatched.remove(&el);
        matched.insert(el.to_owned());
        return Some(el);
    }
    None
}

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
        msoa: &str,
        oas: &HashSet<String>,
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
                        && oas.contains(&household.area)
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
                let area = msoa;
                let age = sample_person.age;
                let sex = sample_person
                    .sex
                    .unwrap_or_else(|| panic!("{hh_type}: expected to have non-missing sex."));
                let eth = sample_person.eth;

                // Try exact match over unmatched
                if let Some(pid) = queues.sample_adult(area, age, sex, eth, &self.p_data) {
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
                    ()
                } else {
                    return Err(anyhow!("No match for: {sample_person:?}").context("sample_hrp()"));
                }
            }
        }
        Ok(())
    }

    fn sample_partner(
        &mut self,
        area: &str,
        oas: &HashSet<String>,
        queues: &mut Queues,
    ) -> anyhow::Result<()> {
        let h2_ref: Vec<_> = self
            .h_data
            .iter_mut()
            .filter(|household| {
                [2, 3].contains(&household.lc4408_c_ahthuk11)
                    && oas.contains(&household.area)
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

            let dis = if let Some(v) = dist_by_ae.get(&(hrp_age, hrp_eth)) {
                v
            } else if let Some(v) = dist_by_a.get(&hrp_age) {
                println!(
                    "Partner-HRP not sampled: {:3}, {:3?}, {:3?} - resample withouth eht.",
                    hrp_age, hrp_sex, hrp_eth
                );
                v
            } else {
                println!(
                    "Partner-HRP not sampled: {}, {:?}, {:?}",
                    hrp_age, hrp_sex, hrp_eth
                );
                &dist
            };

            let partner_sample_id = dis.choose_weighted(&mut self.rng, |item| item.1)?.0;
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
            if let Some(pid) = queues.sample_adult(area, age, sex, eth, &self.p_data) {
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
                ()
            } else {
                println!("No partner match!");
                self.fail += 1;
            }
        }

        Ok(())
    }

    // TODO: add type for LAD
    pub fn run(&mut self) -> anyhow::Result<()> {
        // Create queues
        let mut queues = Queues::new(&self.p_data, &mut self.rng);

        // Deterministic ordering
        let msoas: BTreeSet<String> = self
            .p_data
            .iter()
            .map(|person| person.area.to_owned())
            .collect();

        for msoa in msoas.iter() {
            let oas = self
                .geog_lookup
                .clone()
                .lazy()
                .filter(col("msoa").eq(lit(msoa.to_owned())))
                .select([col("oa")])
                .collect()?;
            let oas: HashSet<String> = oas
                .iter()
                .next()
                .unwrap()
                .str()?
                .into_iter()
                .map(|el| el.unwrap().to_owned())
                .collect();
            println!("{:?}", msoa);
            println!("{:?}", oas);

            // Sample HRP
            self.sample_hrp(msoa, &oas, &mut queues)?;

            // Sample partner
            self.sample_partner(msoa, &oas, &mut queues)?;
        }

        Ok(())
    }
}

struct Queues {
    unmatched: HashSet<PID>,
    matched: HashSet<PID>,
    people_by_area_ase: BTreeMap<(String, Age, Sex, Eth), Vec<PID>>,
    adults_by_area_se: BTreeMap<(String, Sex, Eth), Vec<PID>>,
    adults_by_area_s: BTreeMap<(String, Sex), Vec<PID>>,
    adults_by_area: BTreeMap<String, Vec<PID>>,
}

impl Queues {
    // Construct queues for sampling and matching
    // ---
    // Performance notes:
    //
    // - BTreeMap needed for deterministic shuffling upon iterating
    // HashMap lookups much faster (~2x), consider getting list of sorted keys instead for
    // the shuffle instead and revert to hashmap to get this performance improvement.
    //
    // - String type is used in the lookups. Using &str would be better but there is an
    //   issue with a mutable borrow of person during the assignment of household. An
    //   alternative to String is to use &str and keep a map of PIDs to update after
    //   sampling. This is around (~1.5x).
    // ---
    fn new(p_data: &TiVec<PID, Person>, rng: &mut StdRng) -> Self {
        let unmatched = p_data.iter_enumerated().map(|(idx, _)| idx).collect();
        let mut people_by_area_ase: BTreeMap<(String, Age, Sex, Eth), Vec<PID>> = BTreeMap::new();
        let mut adults_by_area_se: BTreeMap<(String, Sex, Eth), Vec<PID>> = BTreeMap::new();
        let mut adults_by_area_s: BTreeMap<(String, Sex), Vec<PID>> = BTreeMap::new();
        let mut adults_by_area: BTreeMap<String, Vec<PID>> = BTreeMap::new();

        p_data.iter_enumerated().for_each(|(idx, person)| {
            let area = person.area.to_owned();
            let age = person.age;
            let sex = person.sex;
            let eth = person.eth;
            people_by_area_ase
                .entry((area.clone(), age, sex, eth))
                .and_modify(|el| {
                    el.push(idx);
                })
                .or_insert(vec![idx]);
            if age > ADULT_AGE {
                adults_by_area_se
                    .entry((area.clone(), sex, eth))
                    .and_modify(|el| {
                        el.push(idx);
                    })
                    .or_insert(vec![idx]);
                adults_by_area_s
                    .entry((area.clone(), sex))
                    .and_modify(|el| {
                        el.push(idx);
                    })
                    .or_insert(vec![idx]);
                adults_by_area
                    .entry(area.clone())
                    .and_modify(|el| {
                        el.push(idx);
                    })
                    .or_insert(vec![idx]);
            }
        });

        // Shuffle queues
        people_by_area_ase
            .iter_mut()
            .for_each(|(_, v)| v.shuffle(rng));
        adults_by_area_se
            .iter_mut()
            .for_each(|(_, v)| v.shuffle(rng));
        adults_by_area_s
            .iter_mut()
            .for_each(|(_, v)| v.shuffle(rng));
        adults_by_area.iter_mut().for_each(|(_, v)| v.shuffle(rng));

        Self {
            people_by_area_ase,
            adults_by_area_se,
            adults_by_area_s,
            adults_by_area,
            matched: HashSet::new(),
            unmatched,
        }
    }

    fn sample_adult(
        &mut self,
        area: &str,
        age: Age,
        sex: Sex,
        eth: Eth,
        p_data: &TiVec<PID, Person>,
    ) -> Option<PID> {
        let mut pid = None;
        if let Some(v) = self
            .people_by_area_ase
            .get_mut(&(area.to_owned(), age, sex, eth))
        {
            pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.adults_by_area_se.get_mut(&(area.to_owned(), sex, eth)) {
            pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.adults_by_area_s.get_mut(&(area.to_owned(), sex)) {
            pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.adults_by_area.get_mut(&area.to_owned()) {
            // Custom algorithm to get PID with smallest difference in age
            if !v.is_empty() {
                // TODO: this is slow as O(N)
                // Remove any matched elements
                v.retain(|el| !self.matched.contains(el));
                // Get the smallest difference PID
                let (el, _, idx) = v
                    .iter()
                    .enumerate()
                    .map(|(idx, pid)| {
                        let p = p_data
                            .get(*pid)
                            .unwrap_or_else(|| panic!("Invalid PID: {pid}."));
                        (*pid, p.age.0.abs_diff(age.0), idx)
                    })
                    .min_by(|l, r| l.1.cmp(&r.1))
                    .unwrap();
                // TODO: check swap_remove is deterministic
                v.remove(idx);
                self.unmatched.remove(&el);
                self.matched.insert(el.to_owned());
                pid = Some(el);
                println!("{pid:?}: Matched 4");
            }
        }
        pid
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

    // #[test]
    // fn test_sample_hrp() -> anyhow::Result<()> {
    //     let config = Config {
    //         person_resolution: Resolution::MSOA11,
    //         household_resolution: Resolution::OA11,
    //         projection: config::Projection::PPP,
    //         strict: false,
    //         year: Year(2020),
    //         data_dir: PathBuf::from_str("data/microsimulation/data")?,
    //         profile: false,
    //     };
    //     let mut assignment = Assignment::new("E06000001", &config)?;

    //     // Deterministic ordering
    //     let msoas: BTreeSet<String> = assignment
    //         .p_data
    //         .iter()
    //         .map(|person| person.area.to_owned())
    //         .collect();

    //     for msoa in msoas.iter() {
    //         let oas = assignment
    //             .geog_lookup
    //             .clone()
    //             .lazy()
    //             .filter(col("msoa").eq(lit(msoa.to_owned())))
    //             .select([col("oa")])
    //             .collect()?;
    //         let oas: HashSet<String> = oas
    //             .iter()
    //             .next()
    //             .unwrap()
    //             .str()?
    //             .into_iter()
    //             .map(|el| el.unwrap().to_owned())
    //             .collect();
    //         println!("{:?}", msoa);
    //         println!("{:?}", oas);
    //         assignment.sample_hrp(msoa, &oas)?;
    //     }

    //     Ok(())
    // }

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
