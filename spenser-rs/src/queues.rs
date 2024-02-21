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

pub struct Queues {
    pub unmatched: HashSet<PID>,
    pub matched: HashSet<PID>,
    people_by_area_ase: BTreeMap<(MSOA, Age, Sex, Eth), Vec<PID>>,
    adults_by_area_se: BTreeMap<(MSOA, Sex, Eth), Vec<PID>>,
    adults_by_area_s: BTreeMap<(MSOA, Sex), Vec<PID>>,
    adults_by_area: BTreeMap<MSOA, Vec<PID>>,
    children_by_area_se: BTreeMap<(MSOA, Sex, Eth), Vec<PID>>,
    children_by_area_s: BTreeMap<(MSOA, Sex), Vec<PID>>,
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

        p_data.iter_enumerated().for_each(|(idx, person)| {
            let area = person.msoa.to_owned();
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
            } else {
                children_by_area_se
                    .entry((area.clone(), sex, eth))
                    .and_modify(|el| {
                        el.push(idx);
                    })
                    .or_insert(vec![idx]);
                children_by_area_s
                    .entry((area.clone(), sex))
                    .and_modify(|el| {
                        el.push(idx);
                    })
                    .or_insert(vec![idx]);
            }
        });

        // Shuffle queues
        // TODO: rewrite with macro
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
        children_by_area_se
            .iter_mut()
            .for_each(|(_, v)| v.shuffle(rng));
        children_by_area_s
            .iter_mut()
            .for_each(|(_, v)| v.shuffle(rng));

        Self {
            people_by_area_ase,
            adults_by_area_se,
            adults_by_area_s,
            adults_by_area,
            children_by_area_se,
            children_by_area_s,
            matched: HashSet::new(),
            unmatched,
        }
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
