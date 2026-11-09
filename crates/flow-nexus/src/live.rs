//! A value the running Nexus takes from its configuration: read whole as it
//! stands, and replaced whole when a Configure changes it, so the next use
//! sees the new value without a restart. A use already under way keeps the
//! value it began with.

use std::sync::{Arc, RwLock};

pub struct Live<T> {
    value: RwLock<Arc<T>>,
}

pub trait HoldsLiveValue<T>: Sized {
    fn holding(value: T) -> Self;
    /// The value as it stands now.
    fn current(&self) -> Arc<T>;
    /// Replaces the value; every later `current` sees the new one.
    fn replace(&self, value: T);
    /// The value itself, for a holder no other thread can reach.
    fn edit(&mut self) -> &mut T
    where
        T: Clone;
}

impl<T> HoldsLiveValue<T> for Live<T> {
    fn holding(value: T) -> Self {
        Self {
            value: RwLock::new(Arc::new(value)),
        }
    }

    fn current(&self) -> Arc<T> {
        self.value
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn replace(&self, value: T) {
        *self
            .value
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(value);
    }

    fn edit(&mut self) -> &mut T
    where
        T: Clone,
    {
        Arc::make_mut(
            self.value
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Live<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.current().fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::{HoldsLiveValue, Live};

    #[test]
    fn a_replaced_value_is_seen_by_the_next_read_and_not_by_one_held() {
        let live = Live::holding(String::from("first"));
        let held = live.current();
        live.replace(String::from("second"));
        assert_eq!(*held, "first");
        assert_eq!(*live.current(), "second");
    }
}
