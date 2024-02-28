use std::collections::BTreeMap;

use hashbrown::HashSet;
use rand::{rngs::StdRng, seq::SliceRandom};
use typed_index_collections::TiVec;

use crate::{
    person::{Person, PID},
    return_some, Age, Eth, Sex, ADULT_AGE, MSOA,
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

#[derive(Debug)]
pub struct Queues {
    pub unmatched: HashSet<PID>,
    pub matched: HashSet<PID>,
    people_by_area_ase: BTreeMap<(MSOA, Age, Sex, Eth), Vec<PID>>,
    adults_by_area_se: BTreeMap<(MSOA, Sex, Eth), Vec<PID>>,
    adults_by_area_s: BTreeMap<(MSOA, Sex), Vec<PID>>,
    adults_by_area: BTreeMap<MSOA, Vec<PID>>,
    children_by_area_se: BTreeMap<(MSOA, Sex, Eth), Vec<PID>>,
    children_by_area_s: BTreeMap<(MSOA, Sex), Vec<PID>>,
    pub people_by_area_over_75: BTreeMap<MSOA, Vec<PID>>,
    pub people_by_area_19_to_25: BTreeMap<MSOA, Vec<PID>>,
    pub people_by_area_over_16: BTreeMap<MSOA, Vec<PID>>,
}

pub enum AdultOrChild {
    Adult,
    Child,
}

fn get_closest(age: Age, v: &Vec<PID>, p_data: &TiVec<PID, Person>) -> Option<(usize, PID)> {
    // Custom algorithm to get PID with smallest difference in age
    if !v.is_empty() {
        // TODO: possibly improve this as O(N)
        // Get the smallest difference PID
        let (el, _, idx) = v
            .iter()
            .enumerate()
            .map(|(idx, pid)| {
                (
                    *pid,
                    p_data
                        .get(*pid)
                        .unwrap_or_else(|| panic!("Invalid PID: {pid}."))
                        .age
                        .0
                        .abs_diff(age.0),
                    idx,
                )
            })
            .min_by(|l, r| l.1.cmp(&r.1))
            // Unwrap: Cannot be None as v is not empty
            .unwrap();
        Some((idx, el))
    } else {
        None
    }
}

/// Additional operations useful for maps with queues.
trait QueueOperations<K: Ord> {
    /// Add a given `pid` to queue at a given key.
    fn add_pid(&mut self, key: K, pid: PID);
    /// Shuffle a given queue.
    fn shuffle(&mut self, rng: &mut StdRng);
    /// Get an unmatched `pid` given a key and update matched and unmatched sets.
    fn get_sample(
        &mut self,
        key: &K,
        matched: &mut HashSet<PID>,
        unmatched: &mut HashSet<PID>,
    ) -> Option<PID>;
}

impl<K: Ord> QueueOperations<K> for BTreeMap<K, Vec<PID>> {
    fn add_pid(&mut self, key: K, pid: PID) {
        {
            self.entry(key)
                .and_modify(|el| {
                    el.push(pid);
                })
                .or_insert(vec![pid]);
        }
    }
    fn shuffle(&mut self, rng: &mut StdRng) {
        self.iter_mut().for_each(|(_, v)| v.shuffle(rng))
    }
    /// Given an MSOA, return a PID if one exists and update matched and unmatched sets.
    fn get_sample(
        &mut self,
        key: &K,
        matched: &mut HashSet<PID>,
        unmatched: &mut HashSet<PID>,
    ) -> Option<PID> {
        let v = self.get_mut(key).expect("Invalid MSOA.");
        update_pid_vec(v, matched, unmatched)
    }
}

impl Queues {
    // Construct queues for sampling and matching
    // ---
    // Performance notes:
    //
    // - BTreeMap needed for deterministic shuffling upon iterating
    //   HashMap lookups much faster (~2x), consider getting list of sorted keys instead for the
    //   shuffle instead and revert to hashmap to get this performance improvement.
    //
    // - MSOA (wrapped String) type is used in the lookups. Using &str would be better but there is
    //   an issue with a mutable borrow of person during the assignment of household. An alternative
    //   to String is to use &str and keep a map of PIDs to update after sampling. This is (~1.5x).
    // ---
    pub fn new(p_data: &TiVec<PID, Person>, rng: &mut StdRng) -> Self {
        let unmatched = p_data.iter_enumerated().map(|(idx, _)| idx).collect();
        let mut people_by_area_ase: BTreeMap<(MSOA, Age, Sex, Eth), Vec<PID>> = BTreeMap::new();
        let mut adults_by_area_se: BTreeMap<(MSOA, Sex, Eth), Vec<PID>> = BTreeMap::new();
        let mut adults_by_area_s: BTreeMap<(MSOA, Sex), Vec<PID>> = BTreeMap::new();
        let mut adults_by_area: BTreeMap<MSOA, Vec<PID>> = BTreeMap::new();
        let mut children_by_area_se: BTreeMap<(MSOA, Sex, Eth), Vec<PID>> = BTreeMap::new();
        let mut children_by_area_s: BTreeMap<(MSOA, Sex), Vec<PID>> = BTreeMap::new();
        let mut people_by_area_over_75: BTreeMap<MSOA, Vec<PID>> = BTreeMap::new();
        let mut people_by_area_19_to_25: BTreeMap<MSOA, Vec<PID>> = BTreeMap::new();
        let mut people_by_area_over_16: BTreeMap<MSOA, Vec<PID>> = BTreeMap::new();
        p_data.iter_enumerated().for_each(|(pid, person)| {
            let (area, age, sex, eth) =
                (person.msoa.to_owned(), person.age, person.sex, person.eth);
            // Add PID to relevent queues
            people_by_area_ase.add_pid((area.clone(), age, sex, eth), pid);
            if age > Age(75) {
                people_by_area_over_75.add_pid(area.clone(), pid);
            }
            if age > Age(18) && age < Age(26) {
                people_by_area_19_to_25.add_pid(area.clone(), pid);
            }
            if age > Age(16) {
                people_by_area_over_16.add_pid(area.clone(), pid);
            }
            if age > ADULT_AGE {
                adults_by_area_se.add_pid((area.clone(), sex, eth), pid);
                adults_by_area_s.add_pid((area.clone(), sex), pid);
                adults_by_area.add_pid(area.clone(), pid);
            } else {
                children_by_area_se.add_pid((area.clone(), sex, eth), pid);
                children_by_area_s.add_pid((area.clone(), sex), pid);
            }
        });

        // Shuffle queues
        people_by_area_ase.shuffle(rng);
        adults_by_area_se.shuffle(rng);
        adults_by_area_s.shuffle(rng);
        adults_by_area.shuffle(rng);
        children_by_area_se.shuffle(rng);
        children_by_area_s.shuffle(rng);
        people_by_area_over_75.shuffle(rng);
        people_by_area_19_to_25.shuffle(rng);
        people_by_area_over_16.shuffle(rng);

        Self {
            people_by_area_ase,
            adults_by_area_se,
            adults_by_area_s,
            adults_by_area,
            children_by_area_se,
            children_by_area_s,
            people_by_area_over_75,
            people_by_area_19_to_25,
            people_by_area_over_16,
            matched: HashSet::new(),
            unmatched,
        }
    }

    pub fn sample_person_over_75(&mut self, msoa: &MSOA) -> Option<PID> {
        self.people_by_area_over_75
            .get_sample(msoa, &mut self.matched, &mut self.unmatched)
    }
    pub fn sample_person_over_16(&mut self, msoa: &MSOA) -> Option<PID> {
        self.people_by_area_over_16
            .get_sample(msoa, &mut self.matched, &mut self.unmatched)
    }
    pub fn sample_person_19_to_25(&mut self, msoa: &MSOA) -> Option<PID> {
        self.people_by_area_19_to_25
            .get_sample(msoa, &mut self.matched, &mut self.unmatched)
    }

    pub fn sample_adult_any(&mut self, msoa: &MSOA) -> Option<PID> {
        let v = self.adults_by_area.get_mut(msoa).expect("Invalid MSOA.");
        update_pid_vec(v, &mut self.matched, &mut self.unmatched)
    }

    /// TODO: add doc comment
    pub fn sample_person(
        &mut self,
        msoa: &MSOA,
        age: Age,
        sex: Sex,
        eth: Eth,
        adult_or_child: AdultOrChild,
        p_data: &TiVec<PID, Person>,
    ) -> Option<PID> {
        match adult_or_child {
            AdultOrChild::Adult => self.sample_adults(msoa, age, sex, eth, p_data),
            AdultOrChild::Child => self.sample_children(msoa, age, sex, eth, p_data),
        }
    }

    /// TODO: add doc comment
    fn sample_adults(
        &mut self,
        msoa: &MSOA,
        age: Age,
        sex: Sex,
        eth: Eth,
        p_data: &TiVec<PID, Person>,
    ) -> Option<PID> {
        if let Some(v) = self
            .people_by_area_ase
            .get_mut(&(msoa.to_owned(), age, sex, eth))
        {
            let pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.adults_by_area_se.get_mut(&(msoa.to_owned(), sex, eth)) {
            let pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.adults_by_area_s.get_mut(&(msoa.to_owned(), sex)) {
            let pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.adults_by_area.get_mut(&msoa.to_owned()) {
            update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            if let Some((idx, pid)) = get_closest(age, v, p_data) {
                v.remove(idx);
                self.unmatched.remove(&pid);
                self.matched.insert(pid.to_owned());
                return Some(pid);
            }
        }
        None
    }

    /// TODO: add doc comment
    fn sample_children(
        &mut self,
        msoa: &MSOA,
        age: Age,
        sex: Sex,
        eth: Eth,
        p_data: &TiVec<PID, Person>,
    ) -> Option<PID> {
        if let Some(v) = self
            .people_by_area_ase
            .get_mut(&(msoa.to_owned(), age, sex, eth))
        {
            let pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self
            .children_by_area_se
            .get_mut(&(msoa.to_owned(), sex, eth))
        {
            let pid = update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            return_some!(pid);
        }
        if let Some(v) = self.children_by_area_s.get_mut(&(msoa.to_owned(), sex)) {
            update_pid_vec(v, &mut self.matched, &mut self.unmatched);
            if let Some((idx, pid)) = get_closest(age, v, p_data) {
                v.remove(idx);
                self.unmatched.remove(&pid);
                self.matched.insert(pid.to_owned());
                return Some(pid);
            }
        }
        None
    }
}
