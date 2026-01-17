# General
- All comments and documents must be in English.
- Omit unnecessary obvious comments during coding.
- Documents must be concise, well organized into categories, with no duplicated topics or redundant information. Related topics should be organized in close proximity.
- If you don't know a 3rd-party API, look it up on `https://docs.rs/<crate>`.
- Do not run cargo clippy.
- Always use shorter token paths by importing traits or structures.
- Avoid importing namespaces inside functions.

# Test

- Because crate::spsc, mpsc, mpmc module have the same type alias, in the test just use `crossfire::*`, and distinguish the types and functions with `spsc::`, `mpsc::`, `mpmc::` prefix.
- Run test with `make test`. In order to prevent too long output truncated by AI tool, run test with `make test <test_name>` when you have a targeted test case.
- Do not use cargo test to run the test, always use `make test`. Test case cannot be run concurrently with cargo test default param.
- For statement that don't expect to fail, use `expect()` rather than `unwarp()`
