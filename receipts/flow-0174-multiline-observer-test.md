# Flow 0.17.4 multiline observer test receipt

Date: 2026-09-27

This receipt records source-only test evidence. It does not start, deploy, or
contact a live harness.

## Exact test

The tested function is
`herdr::launch::tests::claude_composed_multiline_direct_skill_prompt_is_observed_byte_for_byte`.
It composes the two-line Claude direct prompt, observes the plain direct user
row, requires ordered `spirit` then `main-flow` Skill confirmation, and checks
the receipt. It also keeps the wrapped-direct positive test separate.

The exact 205-line test block, including its `#[test]` attribute, had SHA-256:

`06dc95bafa9d6876d49a6968a6d1d21fee46726c04b42234347611f2cbc3bd22`

That block was copied unchanged into a disposable Jujutsu workspace at the
immutable `flow-0.17.3` revision. Only test-module source was added there; no
production source, commit, bookmark, or remote was changed.

## Baseline witness

Revision: `0b512ee0b6681b1925fee7b6435aa7c2eac26bfb` (`flow-0.17.3`)

Command:

```text
cargo test -p flow-nexus --lib herdr::launch::tests::claude_composed_multiline_direct_skill_prompt_is_observed_byte_for_byte -- --exact
```

Result: exit `101`; 0 passed, 1 failed. The exact test stopped at its first
plain-direct observation with `native Claude first turn loaded no stacked
command`.

## Successor witness

Parent revision: `da58712b1e686f93e0632651d4a1d09b3767941f`

The same command returned exit `0`; 1 passed, 0 failed. The retained wrapped
direct test,
`herdr::launch::tests::claude_pasted_direct_skill_prompt_normalizes_the_wrapper_and_requires_each_skill`,
also returned exit `0`; 1 passed, 0 failed. `cargo fmt --check` returned exit
`0`.

## Refusal oracle

The multiline test first accepts the exact composed body. It then changes only
the one newline byte in the first row to carriage return, preserves the footer,
and proves the resulting body hash differs. That lone altered first row is
refused. A following case places the altered body after the authentic input and
asserts the narrower `first-turn text differs from intent` error. The fixture
also rejects reversed Skill order and a receipt before the second Skill
expansion.
