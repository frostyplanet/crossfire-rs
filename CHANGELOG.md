# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Removed

### Changed

### Fixed

## [3.1.12] - 2026-05-14

### Added

- WeakTx: Add `send_unchecked()` for unbounded channel

### Fixed

- doc: Add notice about buffer channel message dangling

- doc: Add safety notice for deadlock scenario with `WeakTx::upgrade()`

## [3.1.11] - 2026-05-13

### Added

- Implement Async/Blocking Tx/Rx traits for &AsyncTx, &AsyncRx, &MAsyncTx, &MAsyncRx, &Tx,
 &Rx, &MTx, &MRx.

### Removed

- breaking change of Async/Blocking Tx/Rx Trait:

  - Remove Send and 'static (because &AsyncTx, &AsyncRx does not have Send + 'static)

  - Remove `to_stream()` from trait method, use `Into<Pin<Box<dyn Stream>>>` instead.

  - Remove `clone_to_vec()` (which only used by benchmarks)

## [3.1.10] - 2026-05-05

### Fixed

- waitgroup: Avoid miri report on stack borrow rule (issue #66)

## [3.1.9] - 2026-05-05

### Fixed

- Reduce Send requirement in generic (issue #64), which makes the error prompt cleaner

## [3.1.8] - 2026-05-04

### Added

- Add WeakTx. which can downgrade from or upgrade to MTx / MAsyncTx

## [3.1.7] - 2026-03-19

### Added

- oneshot: Add TxOneshot::is_disconnected()

## [3.1.6] - 2026-03-18

### Added

- waitgroup: Add WaitGroupInline (which does not allocate)

## [3.1.4] - 2026-02-25

### Changed

- oneshot: Add Sync for TxOneshot

## [3.1.2] - 2026-02-16

### Changed

- waitgroup: Add inner T inside, just like Arc, this break previous 3.1.0 and 3.1.1

## [3.1.1] - 2026-02-15

### Changed

- waitgroup: Add Sync for WaitGroupGuard

## [3.1.0] - 2026-02-14

### Added

- Add WaitGroup that support async & blocking, with custom threshold.

- oneshot: Add recv_async_timeout & recv_async_with_timer

### Changed

- oneshot: Refactor oneshot and optimize out arc cost. try_recv() now require `&mut self`.

- async_tx/async_rx: Refactor SendTimeoutFuture/RecvTimeoutFuture signature, to remove boxed future usage

## [3.0.6] - 2026-02-11

### Fixed

- Fix multiplex: Ensure all message received before disconnect

## [3.0.5] - 2026-02-11

### Fixed

- Fix msrv to 1.79 (NonZero usage)

- Fix clippy warning and document

## [3.0.4] - 2026-02-03

### Fixed

- Avoid overflow evaluation in generic code

  Remove Send/'static/Unpin limit from Flavor/Queue trait and struct definition,
  add the limit to method.

- Blocking method and struct don't need Unpin.

- Async recv does not need Unpin.

## [3.0.3] - 2026-01-30

### Fixed

- Fix multiplex premature closing

## [3.0.2] - 2026-01-23

### Added

- Add missing into_async() method for blocking tx/rc

## [3.0.1] - 2026-01-22

### Changed

- Remove the mode setting from Multiplex (always use round-robin)

- Add default custom weight for Multiplex select, optimize selection cost (throughput +20%)

## [3.0.0] - 2026-01-18

### Changed

- Disable direct_copy to make miri happy

- Simplify waker cleaning logic

## [3.0.0.beta3] - 2026-01-16

### Change

- New implementation of ArraySpsc & ArrayMpsc, throughput +50%

- New implementation of OneMpsc, minor speed up.

- Change multiplex recv(), try_recv(), recv_timeout() to &self, and impl BlockingRxTrait.

- Remove unused lifetime param in BlockingRxTrait.

### Fixed

Problems from v3 beta

- Add more backoff yielding for One flavor, to ensure 8x1, 16x1 cases stable, and minor optimize.

- Fix commit_waiting state wrong condition, which lead to regression in cases like 1000 async tx.

- Spsc should disable direct_copy (which only safe for MP)

## [3.0.0.beta2] - 2026-01-15

- Fix Array visibility in flavor module

- Fix AsyncTxTrait for compio (The sleep does not have Send)

## [3.0.0.beta1] - 2026-01-14

### Changed

- Change interface to V3 generic flavor API

- Optimize for SPSC

### Added

- Add One flavor for bounded size 1 case

- Add Null flavor for cancellation purpose channel

- Add Select API

- Add Multiplex API

## [2.1.10] - 2026-01-10

### Added

- Add `oneshot` module

- Add test workflow for `compio` (by lisovskiy)

### Changed

- Allow Blocking/Async Tx/Rx trait to be used as trait objects

## [2.1.9] - 2025-12-31

- Fix speed regression on ARM (fix backoff)

## [2.1.8] - 2025-11-08

### Fixed

- Add `#[must_use]` to hint missing await on Future (by MathisWellmann)

## [2.1.7] - 2025-11-08

### Changed

- Depend on `futures-core` crate instead of `futures` (issue #45)

## [2.1.6] - 2025-10-10

### Changed

- Delete the code probing tokio (to prevent an issue in cargo 1.87-1.90 triggering the code without tokio feature enable)

## [2.1.5] - 2025-10-06

### Fixed

- Remove doc_auto_cfg because removal by rust

## [2.1.4] - 2025-10-01

### Changed

- Adjust backoff for Arm  (increase size 1 speed)

- async: Use try_change_state() to reset init instead of get_state(), (Minor improvement on x86 bounded_100_async_n_n)

## [2.1.3] - 2025-09-26

### Added

- Add send_with_timer() and recv_with_timer() for other async runtime (eg. smol).

## [2.1.1-2.1.2]

### Changed

- Minor changed to doc

## [2.1.0] - 2025-9-21

### Changed

- Refactor to drop dependency of crossbeam-channel, the underlayering is modified version of crossbeam-queue.

- Bounded channel speed receive massive boost.

- AsyncTx can convert back and forth with Tx, and AsyncRx can convert back and forth with Rx.

- Optimise for VM machine that only have 1 cpu.

- Use MaybeUninit to optimise the moving of large blob message for bounded channel, in nearly full scenario.

- Rename ReceiveFuture to RecvFuture, ReceiveTimeoutFuture to RecvTimeoutFuture.

### Removed

- Remove AsyncTx::send_blocking() and AsyncRx::recv_blocking(), instead, you can use type conversion into Tx/Rx.

## [2.0.26] - 2025-08-30

### Fixed

- waker_registry: Fix hang detect by miri in cancel_waker(), issue #34

## [2.0.25] - 2025-08-29

### Fixed

- More strict with the waker status, issue #34 (use SeqCst in reset_init)

## [2.0.24] - 2025-08-26

### Fixed

- More strict with the waker status,  issue #34 (spurious wake up, and waker commit)

## [2.0.23] - 2025-08-23

### Fixed

- Change is_disconnected() to SeqCst

## [2.0.22] - 2025-08-21

### Fixed

- RegistryMulti: Fix defend against infinite loop for sink/stream, code introduced from 2.0.20.

## [2.0.21] - 2025-08-21

### Added

- Add clone_to_vec() method in async / blocking tx/rx trait

### Fixed

- AsyncSink: Fix typo in clear waker on drop (Does not affect stability)

## [2.0.20] - 2025-08-17

### Added

- AsyncTxTrait: Add Into<AsyncSink<T>>

- AsyncRxTrait: Add Into<AsyncStream<T>>

### Fixed

- Change the behavior of AsyncSink::poll_send() and AsyncStream::poll_item(), to make sure
stream/sink wakers are notified, preventing deadlock from happening if user wants to cancel the operation.
Add explanation to the document.

- Defend against infinite loop when waking up all wakers, given the change of sink/stream.

## [2.0.19] - 2025-08-13

### Added

- Add capacity()

## [2.0.18] - 2025-08-11

### Fixed

- Change some atomic load ordering from Acquire to SeqCst to pass validation by Miri.

## [2.0.17] - 2025-08-08

### Fixed

- Reuse and cleanup waker as much as possible (for idle select scenario)

- Change some atomic store ordering from Release to SeqCst to avoid further trouble.

## [2.0.16] - 2025-08-04

### Added

- Add into_blocking()

- Add missing into_sink() for MAsyncTx.

- Add From for AsyncSink and AsyncStream.

## [2.0.15] - 2025-08-04

### Added

- Add missing conversion: MAsyncTx->AsyncTx and MTx->Tx

## [2.0.14] - 2025-08-03

### Changed

- Optimise bounded size 1 speed with backoff

- Updated benchmark result vs kanal to wiki

## [2.0.13] - 2025-07-24

### Fixed

- Fix a deadlock https://github.com/frostyplanet/crossfire-rs/issues/22

### Added

- Allow type conversion from AsyncTx -> Tx, AsyncRx -> Rx

## [2.0.12] - 2025-07-18

### Fixed

- Fix a possible hang in LockedQueue introduced from v2.0.5

## [2.0.11] - 2025-07-18

### Added

- Add Deref/AsRef for sender & receiver type to ChannelShared

- Add is_full(), get_tx_count(), get_rx_count()

- Revert the removal of send_blocking() and recv_blocking() (will maintain through 2.0.x)

### Removed

- Remove DerefMut because it's no used.

### Fixed

- Fix send_timeout() in blocking context

## [2.0.10] yanked

published with the wrong branch, do not use.

## [2.0.9] - 2025-07-16

### Added

- Add is_disconnected() to sender and receiver type.

- Add Deref for AsyncSink to AsyncTx, and AsyncStream to AsyncRx, remove duplicated code.

### Fixed

- Fix a rare deadlock, when only one future in async runtime (for example channel async-blocking or blocking-async).
Runtime will spuriously wake up with changed Waker.

### Removed

- Remove send_blocking() & recv_blocking(), which is anti-pattern. (Calling function that blocks might lead to deadlock in async runtime)

## [2.0.8] - 2025-07-14

### Added

- AsyncStream: Add try_recv(), len() & is_empty()

## [2.0.7] - 2025-07-13

### Added

- AsyncStream: Add poll_item() for writing custom future, as a replacement to AsyncRx's poll_item(),
 but without the need of LockedWaker.

- Add AsyncSink::poll_send() for writing custom future, as a replacement to AsyncTx's poll_send(),
 but without the need of LockedWaker.

- Implement Debug & Display for all senders and receivers.

### Remove

- Hide LockedWaker, since AsyncRx::poll_item() and AsyncTx::poll_send() is hidden.

### Changed

- Optimise speed for SPSC & MPSC up to 60% (with WeakCell)

- Add execution time log to test cases.

### Fixed

- Fix LockedQueue empty flag (not affecting usage, just not accurate to internal test cases)

## [2.0.6] - 2025-07-10

### Added

- Support timeout and tested on async-std

### Changed

- mark make_recv_future() & make_send_future() deprecated.

- Change poll_send() & poll_item() to private function.

## [2.0.5] - 2025-07-09

### Added

- Add send_timeout() & recv_timeout() for async context

### Fixed

- AsyncRx: Fix rare case that message left on disconnect

- Fixed document typo and improve description.

### Changed

- Optimise RegistryMulti, with 20%+ speed improved on MPSC / MPMC

## [2.0.4] - 2025-07-08

### Changed

- Remove Sync marker in Tx, Rx, AsyncTx, AsyncRx to prevent misuse with Arc


## [2.0.3] - 2025-07-07

### Changed

- Remove duplicated code.

### Fixed

- AsyncRx should not have Clone.

- Protect against misuse of spsc/mpsc when user should use mpmc (avoiding deadlocks)

## [2.0.2] - 2025-07-05

### Added

- Add channels for blocking context (which equals to crossbeam)

### Changed

- Remove unused Clone for LockedWaker

### Fixed

- spsc: Add missing unsupported size=0 overwrites


## [2.0.1] - 2025-07-03

### Added

- Add timeout API for blocking context (by Zach Schoenberger)

### Changed

- Set min Rust version and edition in alignment with crossbeam (by Zach Schoenberger)

## [2.0.0] - 2025-06-27

### Added

- spsc module

- Benchmark suite written with criterion.

### Changed

- Refactor the API design. Unify sender and receiver types.

- Removal of macro rules and refactor SendWakers & RecvWakers into Enum, thus removal of generic type in Channelshared structure.

- Removal of the spin lock in LockedWaker. Simplifying the logic without losing performance.

- Rewrite the test cases with rstest.

### Removed

- Drop SelectSame module, because of hard to maintain, can be replace with future-select.

## [1.1.0] - 2025-06-19

### Changed

- Migrate repo

From <http://github.com/qingstor/crossfire-rs> to <https://github.com/frostyplanet/crossfire-rs>

- Change rust edition to 2024, re-format the code and fix warnings.


## [1.0.1] - 2023-08-29

### Fixed

- Fix atomic ordering for ARM (Have been tested on some ARM deployment)

## [1.0.0] - 2022-12-03

### Changed

- Format all code and announcing v1.0

- I decided that x86_64 stable after one year test.

## [0.1.7] - 2021-08-22

### Fixed

- tx: Remove redundant old_waker.is_waked() on abandon

## [0.1.6] - 2021-08-21

### Fixed

- mpsc: Fix RxFuture old_waker.abandon in poll_item

## [0.1.5] - 2021-06-28

### Changed

- Replace deprecated compare_and_swap

### Fixed

- SelectSame: Fix close_handler last_index

- Fix fetch_add/sub ordering for ARM  (discovered on test hang)
