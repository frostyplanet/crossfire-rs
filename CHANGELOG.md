# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Removed

### Changed

### Fixed


## [2.0.4]

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

- Change rust edition to 2024, re-format the code and fix warnnings.


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
