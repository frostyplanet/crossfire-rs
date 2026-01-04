use captains_log::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(miri))]
pub const ROUND: usize = 10000;
#[cfg(miri)]
pub const ROUND: usize = 20;

#[cfg(feature = "compio")]
use std::sync::OnceLock;

#[cfg(feature = "compio")]
use compio::dispatcher::Dispatcher;

#[cfg(feature = "compio")]
pub static COMPIO_DISPATCHER: OnceLock<Dispatcher> = OnceLock::new();

pub fn _setup_log() {
    #[cfg(feature = "trace_log")]
    {
        let format = recipe::LOG_FORMAT_THREADED_DEBUG;
        #[cfg(miri)]
        {
            let _ = std::fs::remove_file("/tmp/crossfire_miri.log");
            let file = LogRawFile::new("/tmp", "crossfire_miri.log", Level::Debug, format);
            captains_log::Builder::default()
                .tracing_global()
                .add_sink(file)
                .test()
                .build()
                .expect("log setup");
        }
        #[cfg(not(miri))]
        {
            let ring = ringfile::LogRingFile::new(
                "/tmp/crossfire_ring.log",
                500 * 1024 * 1024,
                Level::Debug,
                format,
            );
            let mut config = Builder::default()
                .signal(signal_consts::SIGINT)
                .signal(signal_consts::SIGTERM)
                .tracing_global()
                .add_sink(ring)
                .add_sink(LogConsole::new(
                    ConsoleTarget::Stdout,
                    Level::Info,
                    recipe::LOG_FORMAT_DEBUG,
                ));
            config.dynamic = true;
            config.build().expect("log_setup");
        }
    }
    #[cfg(not(feature = "trace_log"))]
    {
        let _ = recipe::env_logger("LOG_FILE", "LOG_LEVEL").build().expect("log setup");
    }
}

#[allow(dead_code)]
macro_rules! runtime_block_on {
    ($f: expr) => {{
        #[cfg(feature = "smol")]
        {
            log::info!("run with smol");
            smol::block_on($f)
        }
        #[cfg(feature = "async_std")]
        {
            log::info!("run with async_std");
            async_std::task::block_on($f)
        }
        #[cfg(feature = "tokio")]
        {
            let runtime_flag = std::env::var("SINGLE_THREAD_RUNTIME").unwrap_or("".to_string());
            let mut rt = if runtime_flag.len() > 0 {
                log::info!("run with tokio current thread");
                tokio::runtime::Builder::new_current_thread()
            } else {
                log::info!("run with tokio multi thread");
                tokio::runtime::Builder::new_multi_thread()
            };
            rt.enable_all().build().unwrap().block_on($f)
        }
        #[cfg(feature = "compio")]
        {
            log::info!("run with compio");

            let rt = compio::runtime::Runtime::new().unwrap();
            rt.block_on($f)
        }
    }};
}
pub(super) use runtime_block_on;

#[allow(dead_code)]
macro_rules! async_spawn {
    ($f: expr) => {{
        #[cfg(feature = "smol")]
        {
            smol::spawn($f)
        }
        #[cfg(feature = "async_std")]
        {
            async_std::task::spawn($f)
        }
        #[cfg(feature = "tokio")]
        {
            tokio::spawn($f)
        }
        #[cfg(feature = "compio")]
        {
            let disp = crate::tests::common::COMPIO_DISPATCHER.get_or_init(|| {
                let mut dispatcher = compio::dispatcher::DispatcherBuilder::new();
                if std::env::var("SINGLE_THREAD_RUNTIME").is_ok() {
                    dispatcher = dispatcher.worker_threads(std::num::NonZero::new(1).unwrap());
                } else {
                    dispatcher = dispatcher.worker_threads(std::num::NonZero::new(8).unwrap());
                }
                dispatcher.build().expect("create dispatcher")
            });
            disp.dispatch(move || $f).expect("dispatch")
        }
    }};
}
pub(super) use async_spawn;

#[allow(dead_code)]
macro_rules! async_join_result {
    ($th: expr) => {{
        #[cfg(any(feature = "async_std", feature = "smol"))]
        {
            $th.await
        }
        #[cfg(any(feature = "compio", feature = "tokio"))]
        {
            $th.await.expect("join")
        }
    }};
}
pub(super) use async_join_result;

static DROP_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub trait TestDropMsg: Unpin + Send + 'static {
    fn new(v: usize) -> Self;

    fn get_value(&self) -> usize;
}

pub struct SmallMsg(pub usize);

impl Drop for SmallMsg {
    fn drop(&mut self) {
        DROP_COUNTER.fetch_add(1, Ordering::SeqCst);
    }
}

impl TestDropMsg for SmallMsg {
    fn new(v: usize) -> Self {
        Self(v)
    }

    fn get_value(&self) -> usize {
        self.0
    }
}

pub struct LargeMsg([usize; 4]);

impl TestDropMsg for LargeMsg {
    fn new(v: usize) -> Self {
        Self([v, v, v, v])
    }

    fn get_value(&self) -> usize {
        self.0[0]
    }
}

impl Drop for LargeMsg {
    fn drop(&mut self) {
        DROP_COUNTER.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn get_drop_counter() -> usize {
    DROP_COUNTER.load(Ordering::SeqCst)
}

pub fn reset_drop_counter() {
    DROP_COUNTER.store(0, Ordering::SeqCst);
}

pub async fn sleep(duration: std::time::Duration) {
    #[cfg(feature = "smol")]
    {
        smol::Timer::after(duration).await;
    }
    #[cfg(feature = "async_std")]
    {
        async_std::task::sleep(duration).await;
    }
    #[cfg(feature = "tokio")]
    {
        tokio::time::sleep(duration).await;
    }
    #[cfg(feature = "compio")]
    {
        compio::time::sleep(duration).await;
    }
}

#[cfg(not(feature = "smol"))]
pub async fn timeout<F, T>(duration: std::time::Duration, future: F) -> Result<T, String>
where
    F: std::future::Future<Output = T>,
{
    #[cfg(feature = "async_std")]
    {
        async_std::future::timeout(duration, future)
            .await
            .map_err(|_| format!("Test timed out after {:?}", duration))
    }
    #[cfg(feature = "tokio")]
    {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| format!("Test timed out after {:?}", duration))
    }
    #[cfg(feature = "compio")]
    {
        compio::time::timeout(duration, future)
            .await
            .map_err(|_| format!("Test timed out after {:?}", duration))
    }
}
