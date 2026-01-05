use std::fmt;
use std::future::Future;

use criterion::async_executor::AsyncExecutor;

#[allow(dead_code)]
pub const ONE_MILLION: usize = 1000000;
#[allow(dead_code)]
pub const TEN_THOUSAND: usize = 10000;

#[allow(dead_code)]
pub struct Concurrency {
    pub tx_count: usize,
    pub rx_count: usize,
}

impl fmt::Display for Concurrency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}x{}", self.tx_count, self.rx_count)
    }
}

pub struct BenchExecutor();

impl AsyncExecutor for BenchExecutor {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        #[cfg(feature = "smol")]
        {
            use std::num::NonZero;
            use std::thread;
            let num_threads = thread::available_parallelism().unwrap_or(NonZero::new(1).unwrap());
            unsafe { std::env::set_var("SMOL_THREADS", num_threads.to_string()) };
            smol::block_on(future)
        }
        #[cfg(not(feature = "smol"))]
        {
            #[cfg(feature = "async_std")]
            {
                async_std::task::block_on(future)
            }
            #[cfg(not(feature = "async_std"))]
            {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(future)
            }
        }
    }
}

#[allow(unused_macros)]
macro_rules! async_spawn {
    ($f: expr) => {{
        #[cfg(feature = "smol")]
        {
            smol::spawn($f)
        }
        #[cfg(not(feature = "smol"))]
        {
            #[cfg(feature = "async_std")]
            {
                async_std::task::spawn($f)
            }
            #[cfg(any(feature = "tokio", not(feature = "async_std")))]
            {
                tokio::spawn($f)
            }
        }
    }};
}
pub(super) use async_spawn;

#[allow(unused_macros)]
macro_rules! async_join_result {
    ($th: expr) => {{
        #[cfg(feature = "smol")]
        {
            $th.await
        }
        #[cfg(not(feature = "smol"))]
        {
            #[cfg(feature = "async_std")]
            {
                $th.await
            }
            #[cfg(not(feature = "async_std"))]
            {
                $th.await.expect("join")
            }
        }
    }};
}
pub(super) use async_join_result;

#[allow(dead_code)]
#[inline(always)]
pub fn n_n() -> Vec<(usize, usize)> {
    vec![(2, 2), (4, 4), (8, 8), (16, 16)]
}

#[inline(always)]
pub fn n_1() -> Vec<usize> {
    vec![1, 2, 4, 8, 16]
}
