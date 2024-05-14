//! A priority list to help with backend selection

use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct WeightedValue<T> {
    /// A lower score makes this value more likey to be chosen.
    pub score: usize,
    pub value: T,
}

/// A priority queue of weighted values.
///
/// Acts as a "min heap", returning values with a lower score first.
pub struct PriorityList<T> {
    priority_list: BinaryHeap<Reverse<WeightedValue<T>>>,
}

impl<T: Ord> Default for PriorityList<T> {
    fn default() -> Self {
        Self {
            priority_list: BinaryHeap::new(),
        }
    }
}

impl<T: Ord> PriorityList<T> {
    pub fn new() -> Self {
        Self {
            priority_list: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, value: WeightedValue<T>) {
        self.priority_list.push(Reverse(value));
    }

    pub fn pop(&mut self) -> Option<WeightedValue<T>> {
        if let Some(Reverse(b)) = self.priority_list.pop() {
            Some(b)
        } else {
            None
        }
    }
}

impl<T: Ord> IntoIterator for PriorityList<T> {
    type Item = Reverse<WeightedValue<T>>;
    type IntoIter = std::collections::binary_heap::IntoIter<Reverse<WeightedValue<T>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.priority_list.into_iter()
    }
}

impl<T: Ord> Extend<WeightedValue<T>> for PriorityList<T> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = WeightedValue<T>>,
    {
        self.priority_list.extend(iter.into_iter().map(Reverse))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn min_order_respected() {
        let mut list = PriorityList::new();

        list.push(WeightedValue {
            score: 0,
            value: "a",
        });
        list.push(WeightedValue {
            score: 10,
            value: "b",
        });
        list.push(WeightedValue {
            score: 5,
            value: "c",
        });

        assert_eq!(list.pop().unwrap().value, "a");
        assert_eq!(list.pop().unwrap().value, "c");
        assert_eq!(list.pop().unwrap().value, "b");
        assert!(list.pop().is_none());
    }
}
