//! Lean starts: the parts of a harness start a flow goes without.
//!
//! A launch's effective leave-outs are its Start's, or else its metaflow's
//! configured default. Each turns into the harness's own switch, as Claude
//! Code 2.1.294 and codex-next 0.158.0-alpha.9 were measured to take them
//! (the lean-start catalogue of 2026-10-16): a tool left out is named in
//! `--disallowedTools`, a block or feature is turned off by its flag,
//! environment variable or setting. A leave-out the flow cannot go without
//! (ToolSearch, Bash, and the user settings, which carry Herdr's SessionStart
//! hook that Flow's binding relies on), a skill leave-out with Workflow kept,
//! and a leave-out of the other harness are refused before anything
//! launches.

use crate::generated::starter::{LeanStart, RendersLeanStart};
use signal_flow::{HarnessKind, LeaveOut, LeaveOutRefusal, ToolSet};

/// The tools Claude Code defers to ToolSearch, each named once in the
/// listing: leaving them out removes their names.
const CLAUDE_DEFERRED_TOOLS: [&str; 15] = [
    "CronCreate",
    "CronDelete",
    "CronList",
    "DesignSync",
    "EnterWorktree",
    "ExitWorktree",
    "LSP",
    "Monitor",
    "NotebookEdit",
    "PushNotification",
    "RemoteTrigger",
    "SendMessage",
    "TaskStop",
    "WebFetch",
    "WebSearch",
];

/// Which harness a leave-out belongs to: the one a Start may take it on,
/// and the one a configured default naming it must be keyed by.
pub(crate) trait BelongsToHarness {
    fn harness(&self) -> HarnessKind;
}

impl BelongsToHarness for LeaveOut {
    fn harness(&self) -> HarnessKind {
        match self {
            LeaveOut::CodexSkillListing
            | LeaveOut::CodexAgentsMd
            | LeaveOut::CodexCollaborationMode
            | LeaveOut::CodexEnvironmentContext
            | LeaveOut::CodexPermissions
            | LeaveOut::CodexDeveloperInstructions => HarnessKind::Codex,
            _ => HarnessKind::Claude,
        }
    }
}

impl RendersLeanStart for LeanStart {
    fn refusal(&self) -> Option<LeaveOutRefusal> {
        for leave_out in &self.leave_outs {
            if matches!(leave_out, LeaveOut::UserSettings) {
                return Some(LeaveOutRefusal::Indispensable(leave_out.clone()));
            }
            if leave_out.harness() != self.harness_kind {
                return Some(LeaveOutRefusal::NotForHarness(leave_out.clone()));
            }
            if matches!(leave_out, LeaveOut::SkillTool | LeaveOut::BundledSkills)
                && !self.leave_outs.contains(&LeaveOut::WorkflowTool)
            {
                return Some(LeaveOutRefusal::NeedsWorkflowLeftOut(leave_out.clone()));
            }
        }
        None
    }

    fn arguments(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for leave_out in &self.leave_outs {
            let switches: &[&str] = match leave_out {
                LeaveOut::ConfiguredMcpServers => &["--strict-mcp-config"],
                LeaveOut::SkillTool => &["--disable-slash-commands"],
                LeaveOut::UserSettings => &["--setting-sources", "project"],
                LeaveOut::CodexSkillListing => &["-c", "skills.include_instructions=false"],
                LeaveOut::CodexAgentsMd => &["-c", "project_doc_max_bytes=0"],
                LeaveOut::CodexCollaborationMode => {
                    &["-c", "include_collaboration_mode_instructions=false"]
                }
                LeaveOut::CodexEnvironmentContext => &["-c", "include_environment_context=false"],
                LeaveOut::CodexPermissions => &["-c", "include_permissions_instructions=false"],
                LeaveOut::CodexDeveloperInstructions => &[
                    "-c",
                    "developer_instructions=\"\"",
                    "-c",
                    "features.multi_agent_v2.root_agent_usage_hint_text=\"\"",
                    "-c",
                    "features.multi_agent_v2.multi_agent_mode_hint_text=\"\"",
                ],
                _ => &[],
            };
            arguments.extend(switches.iter().map(|switch| (*switch).to_owned()));
        }
        if self.harness_kind == HarnessKind::Claude
            && self.flow_kind == signal_flow::FlowKind::Simplified
            && !arguments
                .iter()
                .any(|argument| argument == "--strict-mcp-config")
        {
            arguments.push("--strict-mcp-config".into());
        }
        let tools = self.disallowed_tools();
        if !tools.is_empty() {
            arguments.push("--disallowedTools".into());
            arguments.push(tools.join(","));
        }
        // The tool set is configuration: a declared list is passed exactly
        // as declared, and the default passes none.
        let tools = match &self.tool_set {
            ToolSet::Explicit(tools) => Some(tools.clone()),
            ToolSet::Default => None,
        };
        if self.harness_kind == HarnessKind::Claude
            && let Some(tools) = tools
        {
            arguments.push("--tools".into());
            arguments.push(tools.join(","));
        }
        arguments
    }

    fn environment(&self) -> Vec<String> {
        let mut environment = Vec::new();
        if self.harness_kind == HarnessKind::Claude {
            if self.flow_kind == signal_flow::FlowKind::Simplified {
                environment.push("ENABLE_CLAUDEAI_MCP_SERVERS=false".into());
            }
        }
        for leave_out in &self.leave_outs {
            let variable = match leave_out {
                LeaveOut::ClaudeAiConnectors => "ENABLE_CLAUDEAI_MCP_SERVERS=false",
                LeaveOut::ExplorePlanAgents => "CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS=1",
                LeaveOut::WorkflowTool => "CLAUDE_CODE_DISABLE_WORKFLOWS=1",
                LeaveOut::CronTools => "CLAUDE_CODE_DISABLE_CRON=1",
                LeaveOut::BundledSkills => "CLAUDE_CODE_DISABLE_BUNDLED_SKILLS=1",
                LeaveOut::ClaudeMd => "CLAUDE_CODE_DISABLE_CLAUDE_MDS=1",
                LeaveOut::GitInstructions => "CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS=1",
                _ => continue,
            };
            if !environment.iter().any(|known| known == variable) {
                environment.push(variable.to_owned());
            }
        }
        environment
    }

    fn disallowed_tools(&self) -> Vec<String> {
        let mut tools: Vec<String> = Vec::new();
        for leave_out in &self.leave_outs {
            let named: &[&str] = match leave_out {
                LeaveOut::BashTool => &["Bash"],
                LeaveOut::ToolSearch => &["ToolSearch"],
                LeaveOut::AgentTool => &["Agent"],
                LeaveOut::FileTools => &["Read", "Edit", "Write"],
                LeaveOut::WorkflowTool => &["Workflow"],
                LeaveOut::ScheduleWakeupTool => &["ScheduleWakeup"],
                LeaveOut::ReportFindingsTool => &["ReportFindings"],
                LeaveOut::ListAgentsTool => &["ListAgents"],
                LeaveOut::WebTools => &["WebFetch", "WebSearch"],
                LeaveOut::DeferredTools => &CLAUDE_DEFERRED_TOOLS,
                LeaveOut::SkillTool => &["Skill"],
                _ => &[],
            };
            for tool in named {
                if !tools.iter().any(|known| known == tool) {
                    tools.push((*tool).to_owned());
                }
            }
        }
        tools
    }

    fn attribution_off(&self) -> bool {
        self.leave_outs.contains(&LeaveOut::AttributionReminder)
    }

    fn skill_listing_off(&self) -> bool {
        self.leave_outs.contains(&LeaveOut::SkillTool)
    }
}

#[cfg(test)]
mod tests {
    use crate::generated::starter::{LeanStart, RendersLeanStart};
    use signal_flow::{HarnessKind, LeaveOut, LeaveOutRefusal, ToolSet};

    fn lean(harness_kind: HarnessKind, leave_outs: &[LeaveOut]) -> LeanStart {
        LeanStart {
            harness_kind,
            leave_outs: leave_outs.to_vec(),
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: ToolSet::Default,
        }
    }

    #[test]
    fn explicit_empty_claude_tool_set_is_passed_to_harness() {
        let launch = LeanStart {
            harness_kind: HarnessKind::Claude,
            leave_outs: Vec::new(),
            flow_kind: signal_flow::FlowKind::Simplified,
            tool_set: ToolSet::Explicit(Vec::new()),
        };
        assert!(launch.refusal().is_none());
        assert!(
            launch
                .arguments()
                .windows(2)
                .any(|args| args == ["--tools", ""])
        );
        assert!(
            !launch
                .environment()
                .iter()
                .any(|variable| variable.starts_with("CLAUDE_CODE_ARTIFACT"))
        );
    }

    /// The tool set is configuration: a declared list is passed exactly as
    /// declared, whatever the flow's kind, and the default passes no tool
    /// flag at all; Flow adds nothing to either.
    #[test]
    fn a_declared_tool_set_is_passed_exactly_and_the_default_passes_none() {
        for flow_kind in [
            signal_flow::FlowKind::Regular,
            signal_flow::FlowKind::Simplified,
        ] {
            for (tool_set, expected) in [
                (ToolSet::Explicit(Vec::new()), Some("")),
                (
                    ToolSet::Explicit(vec!["Bash".into(), "Read".into()]),
                    Some("Bash,Read"),
                ),
                (
                    ToolSet::Explicit(vec!["Artifact".into(), "Read".into()]),
                    Some("Artifact,Read"),
                ),
                (ToolSet::Default, None),
            ] {
                let launch = LeanStart {
                    harness_kind: HarnessKind::Claude,
                    leave_outs: Vec::new(),
                    flow_kind: flow_kind.clone(),
                    tool_set: tool_set.clone(),
                };
                let arguments = launch.arguments();
                let named = arguments
                    .windows(2)
                    .filter(|pair| pair[0] == "--tools")
                    .map(|pair| pair[1].as_str())
                    .collect::<Vec<_>>();
                assert_eq!(
                    named,
                    expected.into_iter().collect::<Vec<_>>(),
                    "{flow_kind:?} {tool_set:?}: {arguments:?}"
                );
                assert!(
                    !arguments.contains(&"--disallowedTools".to_owned()),
                    "{arguments:?}"
                );
            }
        }
    }

    /// Leaving out the user settings drops Herdr's SessionStart hook, on
    /// which Flow binds the flow; tool allow-lists can leave out Bash and
    /// ToolSearch, and the Agent tool may go for a leaf role.
    #[test]
    fn user_settings_are_indispensable_and_the_agent_tool_is_not() {
        assert_eq!(
            lean(HarnessKind::Claude, &[LeaveOut::UserSettings]).refusal(),
            Some(LeaveOutRefusal::Indispensable(LeaveOut::UserSettings))
        );
        assert_eq!(
            lean(HarnessKind::Claude, &[LeaveOut::AgentTool]).refusal(),
            None
        );
    }

    #[test]
    fn only_settings_are_indispensable_and_other_parts_are_checked() {
        assert_eq!(
            lean(HarnessKind::Claude, &[LeaveOut::ToolSearch]).refusal(),
            None
        );
        assert_eq!(
            lean(HarnessKind::Claude, &[LeaveOut::BashTool]).refusal(),
            None
        );
        assert!(
            lean(HarnessKind::Claude, &[LeaveOut::BashTool])
                .disallowed_tools()
                .contains(&"Bash".to_owned())
        );
        assert!(
            lean(HarnessKind::Claude, &[LeaveOut::ToolSearch])
                .disallowed_tools()
                .contains(&"ToolSearch".to_owned())
        );
        assert_eq!(
            lean(HarnessKind::Claude, &[LeaveOut::SkillTool]).refusal(),
            Some(LeaveOutRefusal::NeedsWorkflowLeftOut(LeaveOut::SkillTool))
        );
        assert_eq!(
            lean(HarnessKind::Codex, &[LeaveOut::GitInstructions]).refusal(),
            Some(LeaveOutRefusal::NotForHarness(LeaveOut::GitInstructions))
        );
        assert_eq!(
            lean(HarnessKind::Claude, &[LeaveOut::CodexAgentsMd]).refusal(),
            Some(LeaveOutRefusal::NotForHarness(LeaveOut::CodexAgentsMd))
        );
        assert_eq!(
            lean(
                HarnessKind::Claude,
                &[LeaveOut::SkillTool, LeaveOut::WorkflowTool]
            )
            .refusal(),
            None
        );
    }

    #[test]
    fn claude_leave_outs_become_its_flags_environment_and_settings() {
        let start = lean(
            HarnessKind::Claude,
            &[
                LeaveOut::SkillTool,
                LeaveOut::WorkflowTool,
                LeaveOut::BundledSkills,
                LeaveOut::ClaudeAiConnectors,
                LeaveOut::GitInstructions,
                LeaveOut::WebTools,
                LeaveOut::DeferredTools,
                LeaveOut::AttributionReminder,
            ],
        );
        assert_eq!(start.refusal(), None);
        let arguments = start.arguments();
        assert!(arguments.contains(&"--disable-slash-commands".to_owned()));
        let tools = &arguments[arguments
            .iter()
            .position(|argument| argument == "--disallowedTools")
            .expect("disallowed tools")
            + 1];
        assert!(tools.starts_with("Skill,Workflow,WebFetch,WebSearch,CronCreate"));
        assert_eq!(tools.matches("WebFetch").count(), 1, "each tool once");
        assert_eq!(
            start.environment(),
            [
                "CLAUDE_CODE_DISABLE_WORKFLOWS=1",
                "CLAUDE_CODE_DISABLE_BUNDLED_SKILLS=1",
                "ENABLE_CLAUDEAI_MCP_SERVERS=false",
                "CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS=1"
            ]
        );
        assert!(start.attribution_off() && start.skill_listing_off());
        assert_eq!(
            lean(HarnessKind::Claude, &[]).arguments(),
            Vec::<String>::new(),
            "a seat configured Regular Default passes no tool flag"
        );
        assert!(lean(HarnessKind::Claude, &[]).environment().is_empty());
    }

    #[test]
    fn codex_leave_outs_become_its_config_overrides() {
        let start = lean(
            HarnessKind::Codex,
            &[LeaveOut::CodexSkillListing, LeaveOut::CodexAgentsMd],
        );
        assert_eq!(start.refusal(), None);
        assert_eq!(
            start.arguments(),
            [
                "-c",
                "skills.include_instructions=false",
                "-c",
                "project_doc_max_bytes=0"
            ]
        );
        assert!(start.environment().is_empty());
    }
}
