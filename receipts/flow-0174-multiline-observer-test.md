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

## Reproducible rerun, 2026-09-27

Fresh isolated Jujutsu workspaces were created directly at each immutable
revision. Before invocation, each printed `jj log --no-graph -r @- -T
'commit_id'` and SHA-256 of the same 205-line test block.

```text
baseline revision:  0b512ee0b6681b1925fee7b6435aa7c2eac26bfb
successor revision: 72dd954870816cb0a78465fbcf841ad7ed885c97
test block SHA-256:  06dc95bafa9d6876d49a6968a6d1d21fee46726c04b42234347611f2cbc3bd22
```

The block was added unchanged to the disposable baseline test module only.
The raw focused command in both workspaces was:

```text
cargo test -p flow-nexus --lib herdr::launch::tests::claude_composed_multiline_direct_skill_prompt_is_observed_byte_for_byte -- --exact
```

Baseline raw result (exit `101`):

```text
running 1 test
test herdr::launch::tests::claude_composed_multiline_direct_skill_prompt_is_observed_byte_for_byte ... FAILED
called `Result::unwrap()` on an `Err` value: "native Claude first turn loaded no stacked command"
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 164 filtered out
```

Successor raw result (exit `0`):

```text
running 1 test
test herdr::launch::tests::claude_composed_multiline_direct_skill_prompt_is_observed_byte_for_byte ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 164 filtered out
```

In the successor workspace, `cargo fmt --check` also exited `0`. Baseline
changes remain uncommitted and unpushed; this receipt is the durable artifact
for the command, revision, block-hash, output, and exit evidence.

## Mutation sensitivity rerun, 2026-09-27

Revision: `f0e574263ee66bb91e3cabbe1abaa873635355e8` in a fresh disposable
workspace. The test source and direct-shape logic were unchanged. The complete
production mutation was this one hunk in `prompt_text_matches_intent`:

```diff
-            .is_some_and(|body| {
-                format!("{:x}", Sha256::digest(body.as_bytes())) == intent.prompt_sha256
-            })
+            .is_some()
```

The focused multiline command was the same command recorded above. It exited
`101` after the accepted case passed and the lone changed-byte first-row case
reached `Observed`; its `unwrap_err()` panicked on that `Ok` value. The raw
relevant output was:

```text
called `Result::unwrap_err()` on an `Ok` value: Observed(NativeTargetReceipt { ... })
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 164 filtered out
mutant_exit=101
```

The omitted `NativeTargetReceipt` fields were temporary fixture paths and
hashes; no result lines were changed. `/usr/bin/time` was unavailable (exit
`127`), so no duration is claimed. The mutant workspace was not committed or
pushed.
