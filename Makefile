RUNTESTCASE = _run_test_case() {                                                  \
    case="$(filter-out $@,$(MAKECMDGOALS))";                                      \
    if [ -n "$${case}" ]; then                                                    \
        RUST_BACKTRACE=full cargo test $${case} -- --nocapture --test-threads=1;  \
    else                                                                          \
        RUST_BACKTRACE=full cargo test -- --nocapture --test-threads=1;           \
    fi  \
}

RUNRELEASECASE = _run_test_release_case() {                                                  \
    case="$(filter-out $@,$(MAKECMDGOALS))";                                      \
    if [ -n "$${case}" ]; then                                                    \
        RUST_BACKTRACE=full cargo test $${case} --release -- --nocapture --test-threads=1;  \
    else                                                                          \
        RUST_BACKTRACE=full cargo test --release -- --nocapture --test-threads=1;                                            \
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
	@${RUNTESTCASE}; _run_test_case
	@echo "Done"

.PHONY: test_release
test_release:
	@${RUNRELEASECASE}; _run_test_release_case

.PHONY: build
build: init
	cargo build

.DEFAULT_GOAL = build
