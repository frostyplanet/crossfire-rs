# Crossfire

[![Build Status](https://github.com/frostyplanet/crossfire-rs/workflows/Rust/badge.svg)](
https://github.com/frostyplanet/crossfire-rs/actions)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](
https://github.com/qignstor/crossfire-rs#license)
[![Cargo](https://img.shields.io/crates/v/crossfire.svg)](
https://crates.io/crates/crossfire)
[![Documentation](https://docs.rs/crossfire/badge.svg)](
https://docs.rs/crossfire)
[![Rust 1.36+](https://img.shields.io/badge/rust-1.36+-lightgray.svg)](
https://www.rust-lang.org)

High-performance lockless spsc/mpsc/mpmc channels.

It supports async contexts, and communication between async and blocking contexts.

The low level is based on crossbeam-queue.

For the concept, please refer to the [wiki](https://github.com/frostyplanet/crossfire-rs/wiki).

## Version history

* V1.0: Released in 2022.12 and used in production.

* V2.0: Released in 2025.6. Refactored the codebase and API
by removing generic types from the ChannelShared type, which made it easier to code with.

* v2.1: Released in 2025.9. Removed the dependency on crossbeam-channel and
implemented with a [modified version of crossbeam-queue](https://github.com/frostyplanet/crossfire-rs/wiki/crossbeam-related), which brings performance
improvements for both async and blocking contexts.


## Performance

Being a lockless channel, crossfire outperforms other async-capable channels.
And thanks to a lighter notification mechanism, in a blocking context, some cases are even
better than the original crossbeam-channel,

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.1.0-2025-09-21/mpsc_size_100_sync.png" alt="mpsc bounded size 100 blocking context">

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.1.0-2025-09-21/mpmc_size_100_sync.png" alt="mpmc bounded size 100 blocking context">

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.1.0-2025-09-21/mpsc_size_100_tokio.png" alt="mpsc bounded size 100 async context">

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.1.0-2025-09-21/mpmc_size_100_tokio.png" alt="mpmc bounded size 100 async context">

More benchmark data is posted on [wiki](https://github.com/frostyplanet/crossfire-rs/wiki/benchmark-v2.1.0-vs-v2.0.26-2025%E2%80%9009%E2%80%9021).

Also, being a lockless channel, the algorithm relies on spinning and yielding. Spinning is good on
multi-core systems, but not friendly to single-core systems (like virtual machines).
So we provide a function `detect_backoff_cfg()` to detect the running platform.
Calling it within the initialization section of your code, will get a 2x performance boost on VPS.

The benchmark is written in the criterion framework, in test-suite sub-directory. You can run the benchmark by:

```
cd test-suite

cargo bench --bench crossfire

# To test with other async runtime, eg smol:

cargo bench --bench crossfire -F smol

```

## Test status

**NOTE**: Because v2.1 has push the speed to a level no one has gone before,
it can put a pure pressure to the async runtime.
Some hidden bug (especially atomic ops on weaker ordering platform) might occur:

The test is placed in test-suite directory, run with:

```
make test
```

<table cellpadding="30">
<tr><th>arch</th><th>runtime</th><th>workflow</th><th>status</th></tr>
<tr>
<td align="center" rowspan="4">x86_64</td>
<td>threaded</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_threaded_x86.yml">cron_master_threaded_x86</a> </td>
<td>STABLE</td>
</tr>
<tr><td>tokio 1.47.1</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_tokio_x86.yml">cron_master_tokio_x86</a></td>
<td>STABLE<br/>
</td>
</tr>
<tr><td>async-std</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_async_std_x86.yml">cron_master_async_std_x86</a></td>
<td>STABLE</td>
</tr>
<tr><td>smol</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_smol_x86.yml">cron_master_smol-x86</a></td>
<td>STABLE</td>
<tr><td align="center" rowspan="4">arm</td>
<td>threaded</td>
<td>
<a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_threaded_arm.yml">cron_master_threaded_arm</a><br/>
</td>
<td>STABLE</td>
</tr>
<tr>
<td>tokio >= 1.48 (<a href="https://github.com/tokio-rs/tokio/pull/7622">tokio PR #7622</a>)
</td>
<td>
<a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_tokio_arm.yml">cron_master_tokio_arm</a><br/>
</td>
<td> SHOULD UPGRADE tokio to 1.48<br/>
STABLE
 </td>
</tr>
<tr>
<td>async-std</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_async_std_arm.yml">cron_master_async_std_arm</a></td>
<td>STABLE</td>
</tr>
<tr>
<td>smol</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_smol_arm.yml">cron_master_smol_arm</a> </td>
<td>STABLE</td>
</tr>
<tr>
<td rowspan="4">miri (emulation)</td>
<td>threaded</td>
<td rowspan="2"><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/miri_tokio.yml">miri_tokio</a><br />
<a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/miri_tokio_cur.yml">miri_tokio_cur</a>
</td>
<td>STABLE</td>
</tr>
<tr><td>tokio</td><td>still verifying </td>
</tr>
<tr><td>async-std</td><td>-</td> <td> (timerfd_create) not supported by miri </td>
</tr>
<tr><td>smol</td><td>-</td> <td> (timerfd_create) not supported by miri </td>
</tr>

</table>

v2.0.26 (legacy):

<table cellpadding="30">
<tr><th>arch</th><th>runtime</th><th>workflow</th><th>status</th></tr>
<tr>
<td align="center" rowspan="3">x86_64</td>
<td>threaded</td>
<td rowspan="3">
<a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_2.0_x86.yml">cron_2.0_x86</a></td>
<td align="center" rowspan="3">PASSED</td>
</tr>
<tr><td>tokio 1.47.1</td>
</tr>
<tr><td>async-std</td>
</tr>
<tr><td align="center" rowspan="3">arm</td>
<td>threaded</td>
<td align="center" rowspan="3">
<a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_2.0_arm.yml">cron_2.0_arm</a>
</td>
<td align="center" rowspan="3">PASSED</td>
<tr>
<td>tokio-1.47.1</td>
</tr>
<tr><td>async-std</td>
</tr>
</table>

### Debugging deadlock issue

**Debug locally**:

Use `--features trace_log` to run the bench or test until it hangs, then press `ctrl+c` or send `SIGINT`,  there will be latest log dump to /tmp/crossfire_ring.log (refer to tests/common.rs `_setup_log()`)

**Debug with github workflow**:  https://github.com/frostyplanet/crossfire-rs/issues/37

## APIs

### Modules and functions

There are 3 modules: [spsc](https://docs.rs/crossfire/latest/crossfire/spsc/index.html), [mpsc](https://docs.rs/crossfire/latest/crossfire/mpsc/index.html), [mpmc](https://docs.rs/crossfire/latest/crossfire/mpmc/index.html), providing functions to allocate different types of channels.

The SP or SC interface is only for non-concurrent operation. It's more memory-efficient than MP or MC implementations, and sometimes slightly faster.

The return types in these 3 modules are different:

* mpmc::bounded_blocking() : (tx blocking, rx blocking)

* mpmc::bounded_async() :  (tx async, rx async)

* mpmc::bounded_tx_async_rx_blocking() : (tx async, rx blocking)

* mpmc::bounded_tx_blocking_rx_async() : (tx blocking, rx async)

* mpmc::unbounded_blocking() : (tx non-blocking, rx blocking)

* mpmc::unbounded_async() : (tx non-blocking, rx async)


> **NOTE** :  For a bounded channel, a 0 size case is not supported yet. (Temporary rewrite as 1 size).

### Types

<table align="center" cellpadding="30">
<tr> <th rowspan="2"> Context </th><th colspan="2" align="center"> Sender (Producer) </th> <th colspan="2" align="center"> Receiver (Consumer) </th> </tr>
<tr> <td> Single </td> <td> Multiple </td><td> Single </td><td> Multiple </td></tr>
<tr><td rowspan="2"> <b>Blocking</b> </td>
<td colspan="2" align="center"> BlockingTxTrait </td>
<td colspan="2" align="center"> BlockingRxTrait </td></tr>
<tr>
<td align="center">Tx </td>
<td align="center">MTx</td>
<td align="center">Rx</td>
<td align="center">MRx</td> </tr>

<tr><td rowspan="2"><b>Async</b></td>
<td colspan="2" align="center">AsyncTxTrait</td>
<td colspan="2" align="center">AsyncRxTrait</td></tr>
<tr>
<td>AsyncTx</td>
<td>MAsyncTx</td>
<td>AsyncRx</td>
<td>MAsyncRx</td></tr>

</table>

For the SP / SC version, `AsyncTx`, `AsyncRx`, `Tx`, and `Rx` are not `Clone` and without `Sync`.
Although can be moved to other threads, but not allowed to use send/recv while in an Arc.
(Refer to the compile_fail examples in the type document).

The benefit of using the SP / SC API is completely lockless waker registration, in exchange for a performance boost.

The sender/receiver can use the `From` trait to convert between blocking and async context
counterparts.

### Error types

Error types are the same as crossbeam-channel:  `TrySendError`, `SendError`, `TryRecvError`, `RecvError`

### Feature flags

 - `tokio`: Enable send_timeout, recv_timeout API for async context, based on `tokio`. And will
 detect the right backoff strategy for the type of runtime (multi-threaded / current-thread).

- `async_std`: Enable send_timeout, recv_timeout API for async context, based on `async-std`.

### Async compatibility

Tested on tokio-1.x and async-std-1.x, crossfire is runtime-agnostic.

The following scenarios are considered:

* The `AsyncTx::send()` and `AsyncRx:recv()` operations are **cancellation-safe** in an async context.
You can safely use the select! macro and timeout() function in tokio/futures in combination with recv().
 On cancellation, [SendFuture] and [RecvFuture] will trigger drop(), which will clean up the state of the waker,
making sure there is no mem-leak and deadlock.
But you cannot know the true result from SendFuture, since it's dropped
upon cancellation. Thus, we suggest using `AsyncTx::send_timeout()` instead.

* When the "tokio" or "async_std" feature is enabled, we also provide two additional functions:

- `AsyncTx::send_timeout()`, which will return the message that failed to be sent in
[SendTimeoutError]. We guarantee the result is atomic.
Alternatively, you can use `AsyncTx::send_with_timer()`.

- `AsyncRx::recv_timeout()`, we guarantee the result is atomic.
Alternatively, you can use `crate::AsyncRx::recv_with_timer()`.

* Between blocking context and async context, and between different async runtime instances.

* The async waker footprint.

When using a multi-producer and multi-consumer scenario, there's a small memory overhead to pass along a `Weak`
reference of wakers.
Because we aim to be lockless, when the sending/receiving futures are canceled (like tokio::time::timeout()),
it might trigger an immediate cleanup if the try-lock is successful, otherwise will rely on lazy cleanup.
(This won't be an issue because weak wakers will be consumed by actual message send and recv).
On an idle-select scenario, like a notification for close, the waker will be reused as much as possible
if poll() returns pending.

## Usage

Cargo.toml:
```toml
[dependencies]
crossfire = "2.1"
```

# Example with tokio::select!

```rust
extern crate crossfire;
use crossfire::*;
#[macro_use]
extern crate tokio;
use tokio::time::{sleep, interval, Duration};

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::bounded_async::<i32>(100);
    for _ in 0..10 {
        let _tx = tx.clone();
        tokio::spawn(async move {
            for i in 0i32..10 {
                let _ = _tx.send(i).await;
                sleep(Duration::from_millis(100)).await;
                println!("sent {}", i);
            }
        });
    }
    drop(tx);
    let mut inv = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = inv.tick() =>{
                println!("tick");
            }
            r = rx.recv() => {
                if let Ok(_i) = r {
                    println!("recv {}", _i);
                } else {
                    println!("rx closed");
                    break;
                }
            }
        }
    }
}
```
