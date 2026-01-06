PRIMARY_TARGET := $(firstword $(MAKECMDGOALS))
ARGS := $(filter-out $(PRIMARY_TARGET), $(MAKECMDGOALS))

RUN_TEST_CASE = _run_test_case() {                                                  \
    case="$(filter-out $ARGS,$(MAKECMDGOALS))";                                      \
    if [ -n "$${WORKFLOW}" ]; then \
        export TEST_FLAG=" -- -q --test-threads=1"; \
    else  \
        export TEST_FLAG=" -- --nocapture --test-threads=1"; \
        export LOG_FILE="/tmp/test_crossfire.log"; \
    fi; \
	RUST_BACKTRACE=full cargo test -p crossfire-test $${ARGS} $${FEATURE_FLAG} $${TEST_FLAG};    \
}

RUN_RELEASE_CASE = _run_test_release_case() {                                                  \
    case="$(filter-out $@,$(MAKECMDGOALS))";                                      \
    if [ -n "$${WORKFLOW}" ]; then \
        export TEST_FLAG=" --release -- -q --test-threads=1"; \
    else  \
        export LOG_FILE="/tmp/test_crossfire.log"; \
        export TEST_FLAG=" --release -- --nocapture --test-threads=1"; \
    fi; \
	RUST_BACKTRACE=full cargo test -p crossfire-test $${ARGS} $${FEATURE_FLAG} $${TEST_FLAG};  \
}

RUN_BENCH = _run_bench() { \
	cd test-suite; \
	cargo bench --bench ${ARGS}; \
}

INSTALL_GITHOOKS = _install_githooks() {                \
    git config core.hooksPath ./git-hooks;              \
}

.PHONY: git-hooks
git-hooks:
	@$(INSTALL_GITHOOKS); _install_githooks

.PHONY: init
init: git-hooks

.PHONY: fmt
fmt: init
	cargo fmt

.PHONY: doc
doc:
	RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features

# usage:
#  make test
#  make test test_async
.PHONY: test
test: init
	@echo "Run test"
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F tokio"; _run_test_case
	@echo "Done"

# test with ringfile for deadlog
.PHONY: test_log
test_log: init
	@echo "Run test"
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F tokio,trace_log"; _run_test_case
	@echo "Done"

.PHONY: test_async_std
test_async_std: init
	@echo "Run test"
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F async_std"; _run_test_case
	@echo "Done"

.PHONY: test_log_async_std
test_log_async_std: init
	@echo "Run test"
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F async_std,trace_log"; _run_test_case
	@echo "Done"

.PHONY: test_release
test_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F tokio"; _run_test_release_case

# test with ringfile for deadlog
.PHONY: test_log_release
test_log_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F tokio,trace_log"; _run_test_release_case

.PHONY: test_async_std_release
test_async_std_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F async_std"; _run_test_release_case

# test with ringfile for deadlog
.PHONY: test_log_async_std_release
test_log_async_std_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F async_std,trace_log"; _run_test_release_case

.PHONY: test_smol
test_smol:
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F smol"; _run_test_case

# test with ringfile for deadlog
.PHONY: test_log_smol
test_log_smol:
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F smol,trace_log"; _run_test_case

.PHONY: test_smol_release
test_smol_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F smol"; _run_test_release_case

# test with ringfile for deadlog
.PHONY: test_log_smol_release
test_log_smol_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F smol,trace_log"; _run_test_release_case

.PHONY: test_compio
test_compio:
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F compio"; _run_test_case

# test with ringfile for deadlog
.PHONY: test_log_compio
test_log_compio:
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F compio,trace_log"; _run_test_case

.PHONY: test_compio_release
test_compio_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F compio"; _run_test_release_case

# test with ringfile for deadlog
.PHONY: test_log_compio_release
test_log_compio_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F compio,trace_log"; _run_test_release_case

.PHONY: test_compio_dispatcher
test_compio_dispatcher:
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F compio_dispatcher"; _run_test_case

# test with ringfile for deadlog
.PHONY: test_log_compio_dispatcher
test_log_compio_dispatcher:
	@${RUN_TEST_CASE}; FEATURE_FLAG="-F compio_dispatcher,trace_log"; _run_test_case

.PHONY: test_compio_dispatcher_release
test_compio_dispatcher_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F compio_dispatcher"; _run_test_release_case

# test with ringfile for deadlog
.PHONY: test_log_compio_dispatcher_release
test_log_compio_dispatcher_release:
	@${RUN_RELEASE_CASE}; FEATURE_FLAG="-F compio_dispatcher,trace_log"; _run_test_release_case

# Usage: make bench crossfire bounded_100_async_1_1
.PHONY: bench
bench:
	@${RUN_BENCH}; _run_bench

.PHONY: build
build: init
	cargo build

.DEFAULT_GOAL = build

# Target name % means that it is a rule that matches anything, @: is a recipe;
# the : means do nothing
%:
	@:
