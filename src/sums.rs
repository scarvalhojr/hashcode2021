use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Add;

pub struct AllSums<T> {
    sums: HashMap<T, Vec<T>>,
}

impl<T: Eq + Default + Hash> Default for AllSums<T> {
    fn default() -> Self {
        let sums = vec![(T::default(), vec![])].into_iter().collect();
        Self { sums }
    }
}

impl<T: Eq + Copy + Hash + Add<Output = T>> AllSums<T> {
    pub fn add(&mut self, value: T) {
        let mut new_sums = HashMap::new();
        if !self.sums.contains_key(&value) {
            new_sums.insert(value, vec![value]);
        }

        for (sum, values) in &self.sums {
            let new_sum = *sum + value;
            if !self.sums.contains_key(&new_sum) {
                let mut new_values = values.clone();
                new_values.push(value);
                new_sums.insert(new_sum, new_values);
            }
        }

        self.sums.extend(new_sums.drain());
    }

    pub fn contains_any<I>(&self, mut range: I) -> bool
    where
        I: Iterator<Item = T>,
    {
        range.any(|value| self.sums.contains_key(&value))
    }

    pub fn get_min_sum_values<I>(&self, mut range: I) -> Option<Vec<T>>
    where
        I: Iterator<Item = T>,
    {
        range.find_map(|s| self.sums.get(&s)).cloned()
    }
}
