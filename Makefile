RUNTESTCASE = _run_test_case() {                                                  \
    case="$(filter-out $@,$(MAKECMDGOALS))";                                      \
    if [ -n "$${WORKFLOW}" ]; then \
        export TEST_FLAG=" -- -q --test-threads=1"; \
    else  \
        export TEST_FLAG=" -- --nocapture --test-threads=1"; \
        export LOG_FILE="/tmp/test_crossfire.log"; \
    fi; \
    if [ -n "$${case}" ]; then                                                    \
        RUST_BACKTRACE=full cargo test $${case} $${FEATURE_FLAG} $${TEST_FLAG};    \
    else  \
        RUST_BACKTRACE=full cargo test $${FEATURE_FLAG} $${TEST_FLAG};           \
    fi;  \
}

RUNRELEASECASE = _run_test_release_case() {                                                  \
    case="$(filter-out $@,$(MAKECMDGOALS))";                                      \
    if [ -n "$${WORKFLOW}" ]; then \
        export TEST_FLAG=" --release -- -q --test-threads=1"; \
    else  \
        export LOG_FILE="/tmp/test_crossfire.log"; \
        export TEST_FLAG=" --release -- --nocapture --test-threads=1"; \
    fi; \
    if [ -n "$${case}" ]; then                                                    \
        RUST_BACKTRACE=full cargo test $${case} $${FEATURE_FLAG} $${TEST_FLAG};  \
    else                                                                          \
        RUST_BACKTRACE=full cargo test $${FEATURE_FLAG} $${TEST_FLAG};                                            \
    fi  \
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

.PHONY: test
test: init
	@echo "Run test"
	@${RUNTESTCASE}; FEATURE_FLAG="-F tokio"; _run_test_case
	@echo "Done"

.PHONY: test
test_log: init
	@echo "Run test"
	@${RUNTESTCASE}; FEATURE_FLAG="-F tokio,trace_log"; _run_test_case
	@echo "Done"

.PHONY: test
test_async_std: init
	@echo "Run test"
	@${RUNTESTCASE}; FEATURE_FLAG="-F async_std"; _run_test_case
	@echo "Done"

.PHONY: test
test_log_async_std: init
	@echo "Run test"
	@${RUNTESTCASE}; FEATURE_FLAG="-F async_std,trace_log"; _run_test_case
	@echo "Done"

.PHONY: test_release
test_release:
	@${RUNRELEASECASE}; FEATURE_FLAG="-F tokio"; _run_test_release_case

.PHONY: test_log_release
test_log_release:
	@${RUNRELEASECASE}; FEATURE_FLAG="-F tokio,trace_log"; _run_test_release_case

.PHONY: test_async_std_release
test_async_std_release:
	@${RUNRELEASECASE}; FEATURE_FLAG="-F async_std"; _run_test_release_case

.PHONY: test_log_async_std_release
test_log_async_std_release:
	@${RUNRELEASECASE}; FEATURE_FLAG="-F async_std,trace_log"; _run_test_release_case

.PHONY: test_smol
test_smol:
	@${RUNTESTCASE}; FEATURE_FLAG="-F smol"; _run_test_case

.PHONY: test_log_smol
test_log_smol:
	@${RUNTESTCASE}; FEATURE_FLAG="-F smol,trace_log"; _run_test_case

.PHONY: test_smol_release
test_smol_release:
	@${RUNRELEASECASE}; FEATURE_FLAG="-F smol"; _run_test_release_case

.PHONY: test_log_smol_release
test_log_smol_release:
	@${RUNRELEASECASE}; FEATURE_FLAG="-F smol,trace_log"; _run_test_release_case

.PHONY: test_compio
test_compio:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio"; _run_test_case

.PHONY: test_log_compio
test_log_compio:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio,trace_log"; _run_test_case
	
.PHONY: test_compio_release
test_compio_release:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio"; _run_test_release_case

.PHONY: test_log_compio_release
test_log_compio_release:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio,trace_log"; _run_test_release_case

.PHONY: test_compio_dispatcher
test_compio_dispatcher:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio_dispatcher"; _run_test_case

.PHONY: test_log_compio_dispatcher
test_log_compio_dispatcher:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio_dispatcher,trace_log"; _run_test_case
	
.PHONY: test_compio_dispatcher_release
test_compio_dispatcher_release:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio_dispatcher"; _run_test_release_case

.PHONY: test_log_compio_dispatcher_release
test_log_compio_dispatcher_release:
	@${RUNTESTCASE}; FEATURE_FLAG="-F compio_dispatcher,trace_log"; _run_test_release_case


.PHONY: build
build: init
	cargo build

.DEFAULT_GOAL = build

# Target name % means that it is a rule that matches anything, @: is a recipe;
# the : means do nothing
%:
	@:
