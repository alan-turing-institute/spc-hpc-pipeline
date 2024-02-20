use std::collections::BTreeMap;

use hashbrown::HashSet;
use rand::{rngs::StdRng, seq::SliceRandom};
use typed_index_collections::TiVec;

use crate::{
    person::{Person, PID},
    return_some, Age, Eth, Sex, ADULT_AGE,
};

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

pub struct Queues {
    pub unmatched: HashSet<PID>,
    pub matched: HashSet<PID>,
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
    pub fn new(p_data: &TiVec<PID, Person>, rng: &mut StdRng) -> Self {
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

    pub fn sample_adult(
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
