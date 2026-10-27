# Claude Code 2.1.294 harness-text provenance

## Captured source

The text used by the first 31 module declarations in
`claude-2.1.294.datom` comes from the Claude Code transcript attachment
`prompt_snapshot` produced by one ordinary interactive CLI run.  The
attachment reports `version: 2.1.294` and `entrypoint: cli`, and carries a
`cliPrefix` plus twelve `systemPrompt` text blocks.  The capture program made
`claude-2.1.294.md` as:

```text
cliPrefix + "\n" + "\n".join(systemPrompt)
```

It made no textual edits.  This is a capture of that version, host
configuration, and then-current remote flags.  It is not a claim that every
Claude Code 2.1.294 invocation composes the same prompt.  The redirected HTTP
wire probe composed a smaller prompt, so it corroborates the serialization
rule but is not the source of these 31 sections.

The capture was made with
`/nix/store/4slw0czfvd70df8a2c7077aqdilf0hgy-claude-code-2.1.294/bin/claude`.
That executable's SHA-256 is
`d54fe02c46b13e4a08cff3c2d1f771707a7dab07caac9e85a2ca1be1c0e30842`;
`claude --version` reported `2.1.294 (Claude Code)`.  The capture artifacts
are held under
`/home/li/primary/flows/63aef7/reports/stock-prompt/`:

| Artifact | SHA-256 | Role |
|---|---|---|
| `probe/interactive-snapshot.json` | `0d58c0cba3ebb8fef6e49a9590c84ba58f7ab14230de26f92e43b249a2d4d97a` | Concrete transcript attachment/tool output containing `cliPrefix`, the twelve blocks, version, entrypoint, and tool metadata |
| `probe/interactive-screen.txt` | `42c21e6de0dc225860c65a6360de2a0615fa77919456837a7e46e2530a311b4c` | Screen witness of the interactive run |
| `probe/extract.py` | `af02b4c255b7de9bd2c8fa3189c56ebd7bfcee2e50f58e694293bdd1dc13aa72` | Extraction and inspection program |
| `claude-2.1.294.md` | `5167184d9162ca10a983ee336290627f374a1b5f381f5916e248441391e743ee` | Exact joined captured text cut into modules |
| `claude-2.1.294-sections.md` | `c9aa01441fa929e209281a20fdbf09cd343d342bf6bbe327a23a1d56454c2438` | Section boundaries and their prompt-block origins |
| `capture-method.md` | `ae8c6d10df935ee81173cf8ef429f215c6dc50e5a05a1f59c1df2fd36bf57578` | Measurement method, tried routes, observations, and limits |
| `claude-help.txt` | `550db4f88ae1d97bf7244ca96dc89554aabd3978241ceb69f79001e816c35f09` | CLI help output documenting the prompt-snapshot feature |

Model self-report was measured separately and was not used to supply any
module text.

## Captured section mapping

Positions below are one-based, end-inclusive positions in the captured
`claude-2.1.294.md`.  Every listed module's text is exactly the indicated
substring.

| Module | Captured section | Disposition |
|---|---|---|
| Identity | `1:1–1:57` | Retained |
| AgentRole | `3:1–3:97` | Injected |
| SecurityPolicy | `5:1–5:459` | Injected |
| Harness | `7:1–7:9` | Injected |
| OutputRendering | `8:1–8:106` | Injected |
| Permissions | `9:1–9:125` | Injected |
| SystemTurns | `10:1–10:225` | Injected |
| PastedContent | `11:1–11:373` | Injected |
| ToolSelection | `12:1–12:136` | Injected |
| CodeReferences | `13:1–13:62` | Injected |
| DynamicBoundary | `14:1–14:34` | Injected |
| CodeStyle | `15:1–15:94` | Injected |
| Pronouns | `16:1–16:357` | Injected |
| Confirmation | `17:1–17:520` | Injected |
| SessionSpecificGuidance | `18:1–18:27` | Injected |
| UserShellCommands | `19:1–19:258` | Injected |
| Skill | `20:1–20:135` | Injected |
| CodeReviewUltra | `21:1–21:498` | Injected |
| Environment | `22:1–22:13` | Injected |
| ModelIdentifiers | `23:1–23:278` | Injected |
| Surfaces | `24:1–24:147` | Injected |
| FastMode | `25:1–25:138` | Injected |
| ContextManagement | `26:1–27:259` | Injected |
| Decisiveness | `28:1–28:277` | Injected |
| ReasoningEffort | `29:1–29:302` | Injected |
| TurnEnding | `31:1–31:293` | Injected |
| Clarification | `33:1–33:792` | Injected |
| EditReversibility | `35:1–35:620` | Injected |
| PartialCompletion | `37:1–37:775` | Injected |
| TokenBudget | `38:1–38:49` | Injected |
| WebSearch | `39:1–39:423` | Retained |

## Behavior-only modules

The other 31 declarations do not claim captured stock text.  They are
behavior-only mappings from the design's Claude Code 2.1.294 configuration
audit: `ClaudeAiConnectors`, `McpServers`, `Agent`, `ExplorePlanAgents`,
`Read`, `Edit`, `Write`, `Workflow`, `ScheduleWakeup`, `ReportFindings`,
`ListAgents`, `Cron`, `WebFetch`, `DesignSync`, `Worktree`, `LSP`, `Monitor`,
`NotebookEdit`, `PushNotification`, `RemoteTrigger`, `SendMessage`,
`TaskStop`, `SlashCommands`, `BundledSkills`, `ClaudeMd`, `GitInstructions`,
`Attribution`, `UserSettings`, `ToolSearch`, `Bash`, and `Artifact`.
They are all declared `Absent`; their tool, flag, setting, or environment
fields express removal behavior rather than transcript text.
