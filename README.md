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

It supports async contexts and bridges the gap between async and blocking contexts.

The low level is based on crossbeam-queue.
For the concept, please refer to the [wiki](https://github.com/frostyplanet/crossfire-rs/wiki).

## Version history

* v1.0: Used in production since 2022.12.

* v2.0: [2025.6] Refactored the codebase and API
by removing generic types from the ChannelShared type, which made it easier to code with.

* v2.1: [2025.9] Removed the dependency on crossbeam-channel
and implemented with [a modified version of crossbeam-queue](https://github.com/frostyplanet/crossfire-rs/wiki/crossbeam-related),
which brings massive performance improvements for both async and blocking contexts.

* v3.0: [2026.1] Refactored API back to generic, with new features like select, because enum dispatch became bottle neck when adding more channel flavor.
async performance has improved, especially +33% for bounded spsc on x86, +20% for one-sized.
Checkout [compat](https://docs.rs/crossfire/latest/crossfire/compat/index.html) for migration from v2.x.


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
Calling it within the initialization section of your code, will get a 2x performance boost on
VPS.

The benchmark is written in the criterion framework. You can run the benchmark by:

``` shell
make bench crossfire
make bench crossfire_select
```

## APIs

### Concurrency Modules

- [spsc](https://docs.rs/crossfire/latest/crossfire/spsc/index.html), [mpsc](https://docs.rs/crossfire/latest/crossfire/mpsc/index.html), [mpmc](https://docs.rs/crossfire/latest/crossfire/mpmc/index.html). Each has different underlying implementation
optimized to its concurrent model.
The SP or SC interface is only for non-concurrent operation. It's more memory-efficient in waker registration,
and has atomic ops cost reduced in the lockless algorithm.

- [oneshot](https://docs.rs/crossfire/latest/crossfire/oneshot/index.html) has its special sender/receiver type because using `Tx` / `Rx` will be too heavy.

- [select](https://docs.rs/crossfire/latest/crossfire/select/index.html):
    - [Select<'a>](https://docs.rs/crossfire/latest/crossfire/select/struct.Select.html): crossbeam-channel style type erased API, borrows receiver address and select with "token"
    - [Multiplex](https://docs.rs/crossfire/latest/crossfire/select/struct.Multiplex.html): Multiplex stream that owns multiple receiver, select from the same type of
    channel flavors, for the same type of message.

### Flavors

The following lockless queues are expose in [flavor](https://docs.rs/crossfire/latest/crossfire/flavor/index.html) module, and each one have type alias in spsc/mpsc/mpmc:

- `List` (which use crossbeam `SegQueue`)
- `Array` (which is an enum that wraps crossbeam `ArrayQueue`, and a `One` if init with size<=1)
  - For a bounded channel, a 0 size case is not supported yet. (rewrite as 1 size).
- `One` (which derives from `ArrayQueue` algorithm, but have better performance in size=1
scenario, because it have two slots to allow reader and writer works concurrently)
- `Null` (See the doc [null](https://docs.rs/crossfire/latest/crossfire/null/index.html)), for cancellation purpose channel, that only wakeup on
closing.

**NOTE** :
Although the name `Array`, `List` are the same between spsc/mpsc/mpmc module,
they are different type alias local to its parent module. We suggest distinguish by
namespace when import for use.

### Channel builder function

Aside from function `bounded_*`, `unbounded_*` which specify the sender / receiver type,
each module has [build()](https://docs.rs/crossfire/latest/crossfire/mpmc/fn.build.html) and [new()](https://docs.rs/crossfire/latest/crossfire/mpmc/fn.new.html) function, which can apply to any channel flavors, and any async/blocking combinations.

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

*Safety*: For the SP / SC version, [AsyncTx], [AsyncRx], [Tx], and [Rx] are not `Clone` and without `Sync`.
Although can be moved to other threads, but not allowed to use send/recv while in an Arc. (Refer to the compile_fail
examples in the type document).

The benefit of using the SP / SC API is completely lockless waker registration, in exchange for a performance boost.

The sender/receiver can use the **`From`** trait to convert between blocking and async context
counterparts (refer to the [example](#example) below)

### Error types

Error types are the same as crossbeam-channel:

`TrySendError`, `SendError`, `SendTimeoutError`, `TryRecvError`, `RecvError`, `RecvTimeoutError`

### Async compatibility

Tested on tokio-1.x and async-std-1.x, crossfire is runtime-agnostic.

The following scenarios are considered:

* The `AsyncTx::send()` and `AsyncRx::recv()` operations are **cancellation-safe** in an async context.
You can safely use the select! macro and timeout() function in tokio/futures in combination with recv().
 On cancellation, `SendFuture` and `RecvFuture` will trigger drop(), which will clean up the state of the waker,
making sure there is no memory-leak and deadlock.
But you cannot know the true result from SendFuture, since it's dropped
upon cancellation. Thus, we suggest using `AsyncTx::send_timeout()` instead.

* When the "tokio" or "async_std" feature is enabled, we also provide two additional functions:

- `AsyncTx::send_timeout()`, which will return the message that failed to be sent in
`SendTimeoutError`. We guarantee the result is atomic. Alternatively, you can use
`AsyncTx::send_with_timer()`.

- `AsyncRx::recv_timeout()`, we guarantee the result is atomic.
Alternatively, you can use `AsyncRx::recv_with_timer()`.

* The waker footprint:

When using a multi-producer and multi-consumer scenario, there's a small memory overhead to pass along a `Weak`
reference of wakers.
Because we aim to be lockless, when the sending/receiving futures are canceled (like tokio::time::timeout()),
it might trigger an immediate cleanup if the try-lock is successful, otherwise will rely on lazy cleanup.
(This won't be an issue because weak wakers will be consumed by actual message send and recv).
On an idle-select scenario, like a notification for close, the waker will be reused as much as possible
if poll() returns pending.

* Handle written future:

The future object created by `AsyncTx::send()`, `AsyncTx::send_timeout()`, `AsyncRx::recv()`,
`AsyncRx::recv_timeout()` is `Sized`. You don't need to put them in `Box`.

If you like to use poll function directly for complex behavior, you can call
`AsyncSink::poll_send()` or `AsyncStream::poll_item()` with Context.

## Usage

Cargo.toml:
```toml
[dependencies]
crossfire = "3.0"
```

### Feature flags

* `compat`: Enable the compat model, which has the same API namespace struct as V2.x

* `tokio`: Enable `send_timeout()`, `recv_timeout()` with tokio sleep function. (conflict
with `async_std` feature)

* `async_std`: Enable send_timeout, recv_timeout with async-std sleep function. (conflict
with `tokio` feature)

* `trace_log`: Development mode, to enable internal log while testing or benchmark, to debug deadlock issues.

### Example

blocking / async sender receiver mixed together

```rust

extern crate crossfire;
use crossfire::*;
#[macro_use]
extern crate tokio;
use tokio::time::{sleep, interval, Duration};

#[tokio::main]
async fn main() {
    let (tx, rx) = mpmc::bounded_async::<usize>(100);
    let mut recv_counter = 0;
    let mut co_tx = Vec::new();
    let mut co_rx = Vec::new();
    const ROUND: usize = 1000;

    let _tx: MTx<mpmc::Array<usize>> = tx.clone().into_blocking();
    co_tx.push(tokio::task::spawn_blocking(move || {
        for i in 0..ROUND {
            _tx.send(i).expect("send ok");
        }
    }));
    co_tx.push(tokio::spawn(async move {
        for i in 0..ROUND {
            tx.send(i).await.expect("send ok");
        }
    }));
    let _rx: MRx<mpmc::Array<usize>> = rx.clone().into_blocking();
    co_rx.push(tokio::task::spawn_blocking(move || {
        let mut count: usize = 0;
        'A: loop {
            match _rx.recv() {
                Ok(_i) => {
                    count += 1;
                }
                Err(_) => break 'A,
            }
        }
        count
    }));
    co_rx.push(tokio::spawn(async move {
        let mut count: usize = 0;
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    count += 1;
                }
                Err(_) => break 'A,
            }
        }
        count
    }));
    for th in co_tx {
        let _ = th.await.unwrap();
    }
    for th in co_rx {
        recv_counter += th.await.unwrap();
    }
    assert_eq!(recv_counter, ROUND * 2);
}
```

## Test status

**NOTE**: Because we has push the speed to a level no one has gone before,
it can put a pure pressure to the async runtime.
Some hidden bug (especially atomic ops on weaker ordering platform) might occur:

The test is placed in test-suite directory, run with:

```
make test
```

<table cellpadding="30">
<tr><th>arch</th><th>runtime</th><th>workflow</th><th>status</th></tr>
<tr>
<td align="center" rowspan="5">x86_64</td>
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
<tr><td>compio</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_compio_x86.yml">cron_master_compio-x86</a></td>
<td>STABLE</td>
</tr>
<tr><td align="center" rowspan="5">arm</td>
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
<td>compio</td>
<td><a href="https://github.com/frostyplanet/crossfire-rs/actions/workflows/cron_master_compio_arm.yml">cron_master_compio_arm</a> </td>
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

### Debugging deadlock issue

**Debug locally**:

Use `--features trace_log` to run the bench or test until it hangs, then press `ctrl+c` or send `SIGINT`,  there will be latest log dump to /tmp/crossfire_ring.log (refer to tests/common.rs `_setup_log()`)

**Debug with github workflow**:  https://github.com/frostyplanet/crossfire-rs/issues/37
