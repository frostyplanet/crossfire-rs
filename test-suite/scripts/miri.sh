#!/bin/bash
# -Zmiri-no-short-fd-operations is to prevent short write perform by miri, which breaks to atomic appending in log
# -Zmiri-permissive-provenance is to disable warning about parking_lot

# By default log is off, if you need to enable, pass the option with the script: --features trace_log

if [ -z "$MIRI_SEED" ]; then
	MIRI_SEED="$(shuf -i 1-1000 -n 1)"
fi
echo "MIRI_SEED" $MIRI_SEED

MIRIFLAGS="$MIRIFLAGS -Zmiri-seed=$MIRI_SEED -Zmiri-disable-isolation -Zmiri-no-short-fd-operations -Zmiri-backtrace=full -Zmiri-permissive-provenance"
export MIRIFLAGS
echo $MIRIFLAGS
# --lib: to skip doctest
RUSTFLAGS="--cfg tokio_unstable" RUST_BACKTRACE=1 cargo +nightly miri test --lib $@ -- --no-capture --test-threads=1
