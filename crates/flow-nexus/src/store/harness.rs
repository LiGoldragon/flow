//! Configured harness installations, one row per installation name.

use meta_signal_flow::Harness;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference, UpgradeFrom, UpgradeOutcome,
};

const TABLE: TableName = TableName::new("flow_nexus_meta_harness_configuration");
const FAMILY: &str = "flow-nexus-meta-harness-configuration";
const SCHEMA: &str = "flow-nexus-meta-harness-configuration-v2";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredHarness {
    pub(crate) harness: Harness,
}

impl EngineRecord for StoredHarness {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.harness.harness_name.clone())
    }
}

impl MemoryDefinition for StoredHarness {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum PieceV1 {
    Text(String),
    Slot(meta_signal_flow::Slot),
    Flagged(FlaggedV1),
    Joined(meta_signal_flow::Joined_Data),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlaggedV1 {
    string: String,
    slot: meta_signal_flow::Slot,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct HarnessV1 {
    harness_name: meta_signal_flow::HarnessName,
    harness_kind: signal_flow::HarnessKind,
    harness_version: meta_signal_flow::HarnessVersion,
    executable: String,
    home: flow_ethos::Home,
    control_socket_path_option: Option<flow_ethos::ControlSocketPath>,
    served_models: Vec<meta_signal_flow::HarnessModel>,
    modules: Vec<meta_signal_flow::HarnessModule>,
    left_out: flow_ethos::LeftOut,
    arguments: Vec<PieceV1>,
    settings: Vec<meta_signal_flow::Setting>,
    parameters: Vec<meta_signal_flow::Setting>,
    unset: Vec<meta_signal_flow::EnvironmentName>,
    pane_profile: meta_signal_flow::PaneProfile,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredHarnessV1 {
    harness: HarnessV1,
}

impl EngineRecord for StoredHarnessV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.harness.harness_name.clone())
    }
}

impl MemoryDefinition for StoredHarnessV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-harness-configuration-v1")
    }
}

impl UpgradeFrom<StoredHarnessV1> for StoredHarness {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredHarnessV1) -> Decision<Self, Self::Refusal> {
        let previous = previous.harness;
        let arguments = previous.arguments.into_iter().map(upgrade_piece).collect();
        Decision::Ready(Self {
            harness: Harness {
                harness_name: previous.harness_name,
                harness_kind: previous.harness_kind,
                harness_version: previous.harness_version,
                executable: previous.executable,
                home: previous.home,
                control_socket_path_option: previous.control_socket_path_option,
                served_models: previous.served_models,
                modules: previous.modules,
                left_out: previous.left_out,
                arguments,
                settings: previous.settings,
                parameters: previous.parameters,
                unset: previous.unset,
                pane_profile: previous.pane_profile,
            },
        })
    }
}

fn upgrade_piece(piece: PieceV1) -> meta_signal_flow::Piece {
    match piece {
        PieceV1::Text(text) => meta_signal_flow::Piece::Text(text),
        PieceV1::Slot(slot) => meta_signal_flow::Piece::Slot(slot),
        PieceV1::Flagged(flagged) => {
            meta_signal_flow::Piece::Flagged(meta_signal_flow::Flagged_Data {
                first_string: flagged.string,
                slot: flagged.slot,
                second_string: String::new(),
            })
        }
        PieceV1::Joined(joined) => meta_signal_flow::Piece::Joined(joined),
    }
}

#[cfg(test)]
mod upgrade_tests {
    use super::{FlaggedV1, PieceV1, upgrade_piece};
    use meta_signal_flow::{Flagged_Data, Piece, Slot};

    #[test]
    fn v1_flagged_arguments_gain_an_empty_after_string() {
        assert_eq!(
            upgrade_piece(PieceV1::Flagged(FlaggedV1 {
                string: "--tools".into(),
                slot: Slot::AllowedTools,
            })),
            Piece::Flagged(Flagged_Data {
                first_string: "--tools".into(),
                slot: Slot::AllowedTools,
                second_string: String::new(),
            })
        );
    }
}

pub(crate) trait UpgradesHarnesses {
    fn upgraded_harnesses(
        &mut self,
        descriptor: TableDescriptor<StoredHarness>,
    ) -> Result<TableReference<StoredHarness>, super::StoreError>;
}

impl UpgradesHarnesses for Engine {
    fn upgraded_harnesses(
        &mut self,
        descriptor: TableDescriptor<StoredHarness>,
    ) -> Result<TableReference<StoredHarness>, super::StoreError> {
        match self.upgrade_table::<StoredHarnessV1, StoredHarness>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(super::StoreError::HarnessUpgrade(format!("{failure:?}")))
            }
        }
    }
}

impl StoredHarness {
    pub(crate) fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(TABLE, FamilyName::new(FAMILY), Self::schema_hash())
            .with_predecessor::<StoredHarnessV1>()
    }
}

/// The behavior units known for each adapter before the versioned stock
/// prompt text is installed. Batch 16 fills the text; the switches and tools
/// are already the complete removable behavior of the legacy leave-outs.
pub(crate) fn seeded_modules(
    kind: flow_ethos::HarnessKind,
) -> Vec<meta_signal_flow::HarnessModule> {
    use meta_signal_flow::{Assignment, HarnessModule, ModuleText, Setting, Switch};
    let module = |name: &str, tools: &[&str], switches: Vec<Switch>| HarnessModule {
        harness_module_name: name.into(),
        module_text: ModuleText::Absent,
        tools: tools.iter().map(|tool| (*tool).into()).collect(),
        switches,
    };
    let environment = |name: &str, value: &str| {
        Switch::Environment(Assignment {
            environment_name: name.into(),
            environment_value: value.into(),
        })
    };
    let setting = |name: &str, value: &str| {
        Switch::Setting(Setting {
            setting_key: name.into(),
            setting_value: value.into(),
        })
    };
    match kind {
        flow_ethos::HarnessKind::Codex => vec![
            module(
                "Skills",
                &[],
                vec![setting("skills.include_instructions", "false")],
            ),
            module("AgentsMd", &[], vec![setting("project_doc_max_bytes", "0")]),
            module(
                "CollaborationMode",
                &[],
                vec![setting("include_collaboration_mode_instructions", "false")],
            ),
            module(
                "EnvironmentContext",
                &[],
                vec![setting("include_environment_context", "false")],
            ),
            module(
                "Permissions",
                &[],
                vec![setting("include_permissions_instructions", "false")],
            ),
            module(
                "DeveloperInstructions",
                &[],
                vec![
                    setting("developer_instructions", "\"\""),
                    setting("features.multi_agent_v2.root_agent_usage_hint_text", "\"\""),
                    setting("features.multi_agent_v2.multi_agent_mode_hint_text", "\"\""),
                ],
            ),
        ],
        flow_ethos::HarnessKind::Claude => vec![
            module(
                "ClaudeAiConnectors",
                &[],
                vec![environment("ENABLE_CLAUDEAI_MCP_SERVERS", "false")],
            ),
            module(
                "McpServers",
                &[],
                vec![Switch::Flag(vec!["--strict-mcp-config".into()])],
            ),
            module("Agent", &["Agent"], vec![]),
            module(
                "ExplorePlanAgents",
                &[],
                vec![environment("CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS", "1")],
            ),
            module("Read", &["Read"], vec![]),
            module("Edit", &["Edit"], vec![]),
            module("Write", &["Write"], vec![]),
            module(
                "Workflow",
                &["Workflow"],
                vec![environment("CLAUDE_CODE_DISABLE_WORKFLOWS", "1")],
            ),
            module("ScheduleWakeup", &["ScheduleWakeup"], vec![]),
            module("ReportFindings", &["ReportFindings"], vec![]),
            module("ListAgents", &["ListAgents"], vec![]),
            module(
                "Cron",
                &["CronCreate", "CronDelete", "CronList"],
                vec![environment("CLAUDE_CODE_DISABLE_CRON", "1")],
            ),
            module("WebFetch", &["WebFetch"], vec![]),
            module("WebSearch", &["WebSearch"], vec![]),
            module("DesignSync", &["DesignSync"], vec![]),
            module("Worktree", &["EnterWorktree", "ExitWorktree"], vec![]),
            module("LSP", &["LSP"], vec![]),
            module("Monitor", &["Monitor"], vec![]),
            module("NotebookEdit", &["NotebookEdit"], vec![]),
            module("PushNotification", &["PushNotification"], vec![]),
            module("RemoteTrigger", &["RemoteTrigger"], vec![]),
            module("SendMessage", &["SendMessage"], vec![]),
            module("TaskStop", &["TaskStop"], vec![]),
            module("Skill", &["Skill"], vec![]),
            module(
                "SlashCommands",
                &[],
                vec![Switch::Flag(vec!["--disable-slash-commands".into()])],
            ),
            module(
                "BundledSkills",
                &[],
                vec![environment("CLAUDE_CODE_DISABLE_BUNDLED_SKILLS", "1")],
            ),
            module(
                "ClaudeMd",
                &[],
                vec![environment("CLAUDE_CODE_DISABLE_CLAUDE_MDS", "1")],
            ),
            module(
                "GitInstructions",
                &[],
                vec![environment("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1")],
            ),
            module(
                "Attribution",
                &[],
                vec![setting("attribution", "{\"commit\":\"\",\"pr\":\"\"}")],
            ),
            module(
                "UserSettings",
                &[],
                vec![Switch::Flag(vec![
                    "--setting-sources".into(),
                    "project".into(),
                ])],
            ),
            module("ToolSearch", &["ToolSearch"], vec![]),
            module("Bash", &["Bash"], vec![]),
            module(
                "Artifact",
                &["Artifact", "ArtifactComments", "ArtifactData"],
                vec![environment("CLAUDE_CODE_ARTIFACT", "0")],
            ),
        ],
    }
}

/// Builds the three harness installation rows carried by the v2 Nexus
/// declaration. This is the cross-family part of the v2-to-v3 upgrade: the
/// Nexus row loses endpoints and pane profiles only after their values have
/// become Harness rows.
pub(super) fn from_legacy_nexus(
    nexus: &super::nexus_configuration::ConfigureNexusV2,
    models: &[meta_signal_flow::ConfigureModel],
) -> Vec<StoredHarness> {
    use flow_ethos::HarnessKind;
    use meta_signal_flow::{
        Composer, Flagged_Data, Harness, HarnessModel, Joined_Data, PaneProfile, Piece, Retraction,
        Setting, Slot,
    };

    let profile = |kind: HarnessKind| {
        let old = nexus
            .harness_profiles
            .iter()
            .find(|profile| profile.harness_kind == kind);
        let (sigils, interrupts, submits) = old
            .map(|profile| {
                (
                    profile.command_sigil_vector.clone(),
                    profile.interrupt_keys.clone(),
                    profile.submit_keys.clone(),
                )
            })
            .unwrap_or_else(|| match kind {
                HarnessKind::Claude => (
                    vec!["/".into(), "!".into(), "#".into()],
                    vec!["esc".into(), "esc".into()],
                    vec!["enter".into()],
                ),
                HarnessKind::Codex => (vec!["/".into(), "!".into()], vec!["esc".into()], vec![]),
            });
        PaneProfile {
            command_sigil_vector: sigils,
            interrupt_keys: interrupts,
            submit_keys: submits,
            compact_line: "/compact".into(),
            composer: match kind {
                HarnessKind::Claude => Composer {
                    glyph: "❯".into(),
                    retraction: Retraction::Keys(vec!["ctrl+c".into()]),
                    composer_lines: 12,
                    retracted_lines: 32,
                },
                HarnessKind::Codex => Composer {
                    glyph: "›".into(),
                    retraction: Retraction::LineByLine(vec!["ctrl+u".into(), "backspace".into()]),
                    composer_lines: 12,
                    retracted_lines: 32,
                },
            },
        }
    };
    let served = |names: &[String]| {
        names
            .iter()
            .map(|name| HarnessModel {
                model_name: name.clone(),
                recorded: name.clone(),
            })
            .collect()
    };
    let codex_arguments = vec![
        Piece::Text("--remote".into()),
        Piece::Joined(Joined_Data {
            string: "unix://".into(),
            slot: Slot::RemoteSocket,
        }),
        Piece::Text("--model".into()),
        Piece::Slot(Slot::LaunchModel),
        Piece::Text("-c".into()),
        Piece::Joined(Joined_Data {
            string: "model_reasoning_effort=".into(),
            slot: Slot::LaunchEffort,
        }),
        Piece::Text("-c".into()),
        Piece::Text("tui.terminal_title=[\"thread-title\"]".into()),
        Piece::Slot(Slot::ModuleSwitches),
        Piece::Flagged(Flagged_Data {
            first_string: "resume".into(),
            slot: Slot::ThreadId,
            second_string: String::new(),
        }),
    ];
    let codex = |name: &str, endpoint: &flow_ethos::CodexEndpoint| StoredHarness {
        harness: Harness {
            harness_name: name.into(),
            harness_kind: HarnessKind::Codex,
            harness_version: "unrecorded".into(),
            executable: endpoint.client_path.clone(),
            home: endpoint.home.clone(),
            control_socket_path_option: Some(endpoint.control_socket_path.clone()),
            served_models: served(&endpoint.model_name_vector),
            modules: seeded_modules(HarnessKind::Codex),
            left_out: vec![],
            arguments: codex_arguments.clone(),
            settings: vec![Setting {
                setting_key: "shell_environment_policy.inherit".into(),
                setting_value: "\"core\"".into(),
            }],
            parameters: vec![
                Setting {
                    setting_key: "approvalPolicy".into(),
                    setting_value: "\"never\"".into(),
                },
                Setting {
                    setting_key: "sandbox".into(),
                    setting_value: "\"danger-full-access\"".into(),
                },
                Setting {
                    setting_key: "ephemeral".into(),
                    setting_value: "false".into(),
                },
            ],
            unset: vec![],
            pane_profile: profile(HarnessKind::Codex),
        },
    };
    let codex_models = nexus
        .stable_codex
        .model_name_vector
        .iter()
        .chain(nexus.next_codex.model_name_vector.iter())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let claude_models = models
        .iter()
        .map(|model| model.model_name.clone())
        .filter(|model| !codex_models.contains(model))
        .collect::<Vec<_>>();
    let claude_home = std::env::var("CLAUDE_CONFIG_DIR").unwrap_or_else(|_| {
        std::env::var("HOME")
            .map(|home| {
                std::path::Path::new(&home)
                    .join(".claude")
                    .to_string_lossy()
                    .into_owned()
            })
            .unwrap_or_else(|_| ".claude".into())
    });
    let claude = StoredHarness {
        harness: Harness {
            harness_name: "claude".into(),
            harness_kind: HarnessKind::Claude,
            harness_version: "unrecorded".into(),
            executable: "claude".into(),
            home: claude_home,
            control_socket_path_option: None,
            served_models: served(&claude_models),
            modules: seeded_modules(HarnessKind::Claude),
            left_out: vec![],
            arguments: vec![
                Piece::Text("--settings".into()),
                Piece::Slot(Slot::SettingsText),
                Piece::Text("--remote-control".into()),
                Piece::Slot(Slot::Title),
                Piece::Text("--name".into()),
                Piece::Slot(Slot::Title),
                Piece::Text("--system-prompt-file".into()),
                Piece::Slot(Slot::PromptFile),
                Piece::Flagged(Flagged_Data {
                    first_string: "--session-id".into(),
                    slot: Slot::SessionId,
                    second_string: String::new(),
                }),
                Piece::Text("--model".into()),
                Piece::Slot(Slot::LaunchModel),
                Piece::Text("--effort".into()),
                Piece::Slot(Slot::LaunchEffort),
                Piece::Slot(Slot::ModuleSwitches),
                Piece::Flagged(Flagged_Data {
                    first_string: "--disallowedTools".into(),
                    slot: Slot::DeniedTools,
                    second_string: String::new(),
                }),
                Piece::Flagged(Flagged_Data {
                    first_string: "--tools".into(),
                    slot: Slot::AllowedTools,
                    second_string: String::new(),
                }),
            ],
            settings: vec![],
            parameters: vec![],
            unset: vec![
                "CLAUDE_CODE_CHILD_SESSION".into(),
                "CLAUDE_JOB_DIR".into(),
                "CLAUDE_CODE_SESSION_ID".into(),
                "CLAUDE_CODE_SESSION_KIND".into(),
            ],
            pane_profile: profile(HarnessKind::Claude),
        },
    };
    vec![
        codex("codexStable", &nexus.stable_codex),
        codex("codexNext", &nexus.next_codex),
        claude,
    ]
}

#[cfg(test)]
mod tests {
    use super::RendersProductionHarnessConfiguration as _;
    use datom_codec::{Actualizing, Budget, Potential};
    use protos::ReaderBudget;

    fn budget() -> Budget {
        Budget {
            remaining: 262_144,
            reader: ReaderBudget { remaining: 262_144 },
            depth: 0,
            maximum_depth: 1_024,
        }
    }

    #[test]
    fn claude_2_1_294_seed_is_one_complete_harness() {
        let harness = Potential::<meta_signal_flow::Harness>::from(include_str!(
            "../../../../config/harness/claude-2.1.294.datom"
        ))
        .actualize(&mut budget())
        .expect("Claude 2.1.294 Harness seed composes");
        assert_eq!(harness.harness_version, "2.1.294");
        assert_eq!(harness.modules.len(), 62);
        assert!(harness.modules.iter().any(|module| {
            module.harness_module_name == "CodeStyle"
                && matches!(
                    &module.module_text,
                    meta_signal_flow::ModuleText::Injected(text)
                        if text == "Write code that reads like the surrounding code: match its comment density, naming, and idiom."
                )
        }));
        assert!(!harness.left_out.iter().any(|name| name == "Skill"));
        assert!(harness.left_out.iter().any(|name| name == "CodeStyle"));
    }

    #[test]
    fn production_harnesses_join_only_typed_installation_overrides() {
        let rendered = "[ { claude /bin/claude /home/claude None [ { claude-opus claude-opus-recorded } ] } { codexStable /bin/codex-stable /home/stable Some./run/stable.sock [ { gpt-sol gpt-sol-recorded } ] } { codexNext /bin/codex-next /home/next Some./run/next.sock [ { gpt-astra gpt-astra-recorded } ] } ]"
            .render_production_harness_configuration()
            .expect("complete typed overrides render");
        let query = Potential::<meta_signal_flow::Query>::from(rendered)
            .actualize(&mut budget())
            .expect("rendered query composes");
        let meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Harnesses(
            harnesses,
        )) = query
        else {
            panic!("renderer returns one Harnesses configuration query")
        };
        assert_eq!(harnesses.len(), 3);
        let claude = harnesses
            .iter()
            .find(|harness| harness.harness_name == "claude")
            .expect("Claude definition");
        assert_eq!(claude.harness_version, "2.1.294");
        assert_eq!(claude.executable, "/bin/claude");
        assert!(claude.control_socket_path_option.is_none());
        assert_eq!(claude.modules.len(), 62);
        let next = harnesses
            .iter()
            .find(|harness| harness.harness_name == "codexNext")
            .expect("Codex next definition");
        assert_eq!(next.harness_version, "0.161.0-alpha.2");
        assert_eq!(
            next.control_socket_path_option.as_deref(),
            Some("/run/next.sock")
        );
        assert!(next.arguments.iter().any(|piece| matches!(
            piece,
            meta_signal_flow::Piece::Joined(joined)
                if joined.slot == meta_signal_flow::Slot::RemoteSocket
        )));
        assert_eq!(next.served_models[0].recorded, "gpt-astra-recorded");
    }

    #[test]
    fn production_harnesses_refuse_incomplete_ambiguous_or_mistyped_overrides() {
        let valid = "{ claude /bin/claude /home/claude None [ { claude-opus claude-opus } ] }";
        assert!(
            format!("[ {valid} ]")
                .render_production_harness_configuration()
                .unwrap_err()
                .contains("missing production harness")
        );
        assert!(
            format!("[ {valid} {valid} ]")
                .render_production_harness_configuration()
                .unwrap_err()
                .contains("named twice")
        );
        assert!(
            "[ { other /bin/other /home None [] } ]"
                .render_production_harness_configuration()
                .unwrap_err()
                .contains("unknown production harness")
        );
        assert!(
            "[ { claude /bin/claude /home None [ { shared shared } ] } { codexStable /bin/stable /home Some./run/stable [ { shared shared } ] } { codexNext /bin/next /home Some./run/next [] } ]"
                .render_production_harness_configuration()
                .unwrap_err()
                .contains("more than one production harness")
        );
        assert!(
            "[ { claude /bin/claude /home Some./run/wrong [] } { codexStable /bin/stable /home Some./run/stable [] } { codexNext /bin/next /home Some./run/next [] } ]"
                .render_production_harness_configuration()
                .unwrap_err()
                .contains("Claude production harness has no control socket")
        );
    }
}

struct ProductionHarnessDefinitions;

trait DefinesProductionHarnesses {
    fn pane_profile(&self, kind: flow_ethos::HarnessKind) -> meta_signal_flow::PaneProfile;
    fn codex(&self, name: &str, version: &str) -> meta_signal_flow::Harness;
    fn claude(&self) -> Result<meta_signal_flow::Harness, String>;
}

impl DefinesProductionHarnesses for ProductionHarnessDefinitions {
    fn pane_profile(&self, kind: flow_ethos::HarnessKind) -> meta_signal_flow::PaneProfile {
        use meta_signal_flow::{Composer, PaneProfile, Retraction};
        PaneProfile {
            command_sigil_vector: match kind {
                flow_ethos::HarnessKind::Claude => vec!["/".into(), "!".into(), "#".into()],
                flow_ethos::HarnessKind::Codex => vec!["/".into(), "!".into()],
            },
            interrupt_keys: match kind {
                flow_ethos::HarnessKind::Claude => vec!["esc".into(), "esc".into()],
                flow_ethos::HarnessKind::Codex => vec!["esc".into()],
            },
            submit_keys: match kind {
                flow_ethos::HarnessKind::Claude => vec!["enter".into()],
                flow_ethos::HarnessKind::Codex => vec![],
            },
            compact_line: "/compact".into(),
            composer: match kind {
                flow_ethos::HarnessKind::Claude => Composer {
                    glyph: "❯".into(),
                    retraction: Retraction::Keys(vec!["ctrl+c".into()]),
                    composer_lines: 12,
                    retracted_lines: 32,
                },
                flow_ethos::HarnessKind::Codex => Composer {
                    glyph: "›".into(),
                    retraction: Retraction::LineByLine(vec!["ctrl+u".into(), "backspace".into()]),
                    composer_lines: 12,
                    retracted_lines: 32,
                },
            },
        }
    }

    fn codex(&self, name: &str, version: &str) -> meta_signal_flow::Harness {
        use meta_signal_flow::{Flagged_Data, Harness, Joined_Data, Piece, Setting, Slot};
        Harness {
            harness_name: name.into(),
            harness_kind: flow_ethos::HarnessKind::Codex,
            harness_version: version.into(),
            executable: String::new(),
            home: String::new(),
            control_socket_path_option: None,
            served_models: vec![],
            modules: seeded_modules(flow_ethos::HarnessKind::Codex),
            left_out: vec![],
            arguments: vec![
                Piece::Text("--remote".into()),
                Piece::Joined(Joined_Data {
                    string: "unix://".into(),
                    slot: Slot::RemoteSocket,
                }),
                Piece::Text("--model".into()),
                Piece::Slot(Slot::LaunchModel),
                Piece::Text("-c".into()),
                Piece::Joined(Joined_Data {
                    string: "model_reasoning_effort=".into(),
                    slot: Slot::LaunchEffort,
                }),
                Piece::Text("-c".into()),
                Piece::Text("tui.terminal_title=[\"thread-title\"]".into()),
                Piece::Slot(Slot::ModuleSwitches),
                Piece::Flagged(Flagged_Data {
                    first_string: "resume".into(),
                    slot: Slot::ThreadId,
                    second_string: String::new(),
                }),
            ],
            settings: vec![Setting {
                setting_key: "shell_environment_policy.inherit".into(),
                setting_value: "\"core\"".into(),
            }],
            parameters: vec![
                Setting {
                    setting_key: "approvalPolicy".into(),
                    setting_value: "\"never\"".into(),
                },
                Setting {
                    setting_key: "sandbox".into(),
                    setting_value: "\"danger-full-access\"".into(),
                },
                Setting {
                    setting_key: "ephemeral".into(),
                    setting_value: "false".into(),
                },
            ],
            unset: vec![],
            pane_profile: self.pane_profile(flow_ethos::HarnessKind::Codex),
        }
    }

    fn claude(&self) -> Result<meta_signal_flow::Harness, String> {
        use datom_codec::{Actualizing as _, Budget, Potential};
        use protos::ReaderBudget;
        let mut budget = Budget {
            remaining: 262_144,
            reader: ReaderBudget { remaining: 262_144 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<meta_signal_flow::Harness>::from(include_str!(
            "../../../../config/harness/claude-2.1.294.datom"
        ))
        .actualize(&mut budget)
        .map_err(|error| format!("Flow's Claude harness definition is invalid: {error:?}"))
    }
}

/// Joins deployment-owned installation paths and bindings to Flow's three
/// complete production harness definitions and renders one typed Configure
/// query. Unknown, missing or repeated installations and model overlap are
/// refused before any configuration request exists.
pub trait RendersProductionHarnessConfiguration {
    fn render_production_harness_configuration(&self) -> Result<String, String>;
}

impl RendersProductionHarnessConfiguration for str {
    fn render_production_harness_configuration(&self) -> Result<String, String> {
        use crate::generated::library::HarnessOverride;
        use datom_codec::{Actualizing as _, Budget, Datomizable as _, Potential};
        use protos::{Compactable as _, Protosizable as _, ReaderBudget};
        use std::collections::{BTreeMap, BTreeSet};

        let mut budget = Budget {
            remaining: 262_144,
            reader: ReaderBudget { remaining: 262_144 },
            depth: 0,
            maximum_depth: 1_024,
        };
        let overrides = Potential::<Vec<HarnessOverride>>::from(self)
            .actualize(&mut budget)
            .map_err(|error| format!("invalid harness overrides datom: {error:?}"))?;
        let mut by_name = BTreeMap::new();
        for installation in overrides {
            let name = installation.harness_name.clone();
            if !matches!(name.as_str(), "claude" | "codexStable" | "codexNext") {
                return Err(format!("unknown production harness: {name}"));
            }
            if by_name.insert(name.clone(), installation).is_some() {
                return Err(format!("production harness named twice: {name}"));
            }
        }
        for name in ["claude", "codexStable", "codexNext"] {
            if !by_name.contains_key(name) {
                return Err(format!("missing production harness: {name}"));
            }
        }
        let mut models = BTreeSet::new();
        for installation in by_name.values() {
            for model in &installation.harness_model_vector {
                if !models.insert(model.model_name.clone()) {
                    return Err(format!(
                        "model served by more than one production harness: {}",
                        model.model_name
                    ));
                }
            }
        }

        let definitions = ProductionHarnessDefinitions;
        let mut harnesses = vec![
            definitions.claude()?,
            definitions.codex("codexStable", "0.153.4"),
            definitions.codex("codexNext", "0.161.0-alpha.2"),
        ];
        for harness in &mut harnesses {
            let installation = by_name
                .remove(&harness.harness_name)
                .expect("all three names established");
            match harness.harness_kind {
                flow_ethos::HarnessKind::Claude
                    if installation.control_socket_path_option.is_some() =>
                {
                    return Err("Claude production harness has no control socket".into());
                }
                flow_ethos::HarnessKind::Codex
                    if installation.control_socket_path_option.is_none() =>
                {
                    return Err(format!(
                        "Codex production harness requires a control socket: {}",
                        harness.harness_name
                    ));
                }
                _ => {}
            }
            harness.executable = installation.executable;
            harness.home = installation.home;
            harness.control_socket_path_option = installation.control_socket_path_option;
            harness.served_models = installation.harness_model_vector;
        }
        Ok(
            meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Harnesses(
                harnesses,
            ))
            .datomize(vec![])
            .protosize()
            .compact(),
        )
    }
}
