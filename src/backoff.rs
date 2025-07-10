const SPIN_LIMIT: u32 = 6;

pub struct Backoff {
    step: u32,
    limit: u32,
}

impl Backoff {
    #[inline(always)]
    pub fn new(limit: u32) -> Self {
        Self { step: 0, limit }
    }

    #[inline(always)]
    pub fn snooze(&mut self) {
        if self.step <= SPIN_LIMIT {
            for _ in 0..1 << self.step {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
        if self.step < self.limit {
            self.step += 1;
        }
    }

    #[inline(always)]
    pub fn is_completed(&self) -> bool {
        self.step >= self.limit
    }

    #[inline(always)]
    pub fn step(&self) -> usize {
        self.step as usize
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.step = 0;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_backoff() {
        let backoff = Backoff::new(0);
        assert!(backoff.is_completed())
    }
}
