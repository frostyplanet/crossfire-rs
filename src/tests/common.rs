#[allow(dead_code)]
macro_rules! runtime_block_on {
    ($f: expr) => {{
        #[cfg(feature = "async_std")]
        {
            async_std::task::block_on($f);
        }
        #[cfg(not(feature = "async_std"))]
        {
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
            rt.block_on($f);
        }
    }};
}
pub(super) use runtime_block_on;

#[allow(dead_code)]
macro_rules! async_spawn {
    ($f: expr) => {{
        #[cfg(feature = "async_std")]
        {
            async_std::task::spawn($f)
        }
        #[cfg(not(feature = "async_std"))]
        {
            tokio::spawn($f)
        }
    }};
}
pub(super) use async_spawn;

pub async fn sleep(duration: std::time::Duration) {
    #[cfg(feature = "async_std")]
    {
        async_std::task::sleep(duration).await;
    }
    #[cfg(not(feature = "async_std"))]
    {
        tokio::time::sleep(duration).await;
    }
}
