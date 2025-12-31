# General
- All comment and document must be in English.
- Omit unnecessary obvious comment during coding.
- For the document, must be concise, well organize into catelog, no duplicated topic, no redundant information. Relative topic should be organize in near position.
- If you don't know 3rd-party API, should lookup on `https://docs.rs/<crate>`.
- Do not run cargo clippy
- Run test with `make test`. In order to prevent too long output truncated by AI tool, run test with `make test <test_name>` when you have a targeted test case.
- Always use shorter token path by importing the trait or structure.

# Execution plan

Create execution plan with steps when going large design, store into `docs/(topic)_steps.md`, wait for review.
