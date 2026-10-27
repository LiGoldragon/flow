//! Configured harness installations, one row per installation name.

use meta_signal_flow::Harness;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};

const TABLE: TableName = TableName::new("flow_nexus_meta_harness_configuration");
const FAMILY: &str = "flow-nexus-meta-harness-configuration";
const SCHEMA: &str = "flow-nexus-meta-harness-configuration-v1";

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
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA)
    }
}

impl StoredHarness {
    pub(crate) fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(TABLE, FamilyName::new(FAMILY), Self::schema_hash())
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
        Composer, Flagged_Data, Harness, HarnessModel, Joined_Data, PaneProfile, Piece,
        Retraction, Setting, Slot,
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
                HarnessKind::Codex => (
                    vec!["/".into(), "!".into()],
                    vec!["esc".into()],
                    vec![],
                ),
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
                    retraction: Retraction::LineByLine(vec![
                        "ctrl+u".into(),
                        "backspace".into(),
                    ]),
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
        Piece::Joined(Joined_Data { string: "unix://".into(), slot: Slot::RemoteSocket }),
        Piece::Text("--model".into()),
        Piece::Slot(Slot::LaunchModel),
        Piece::Text("-c".into()),
        Piece::Joined(Joined_Data { string: "model_reasoning_effort=".into(), slot: Slot::LaunchEffort }),
        Piece::Text("-c".into()),
        Piece::Text("tui.terminal_title=[\"thread-title\"]".into()),
        Piece::Slot(Slot::ModuleSwitches),
        Piece::Flagged(Flagged_Data { string: "resume".into(), slot: Slot::ThreadId }),
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
            modules: vec![],
            left_out: vec![],
            arguments: codex_arguments.clone(),
            settings: vec![Setting { setting_key: "shell_environment_policy.inherit".into(), setting_value: "\"core\"".into() }],
            parameters: vec![
                Setting { setting_key: "approvalPolicy".into(), setting_value: "\"never\"".into() },
                Setting { setting_key: "sandbox".into(), setting_value: "\"danger-full-access\"".into() },
                Setting { setting_key: "ephemeral".into(), setting_value: "false".into() },
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
        .map(|model| model.native.clone())
        .filter(|model| !codex_models.contains(model))
        .collect::<Vec<_>>();
    let claude_home = std::env::var("CLAUDE_CONFIG_DIR").unwrap_or_else(|_| {
        std::env::var("HOME")
            .map(|home| std::path::Path::new(&home).join(".claude").to_string_lossy().into_owned())
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
            modules: vec![],
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
                Piece::Flagged(Flagged_Data { string: "--session-id".into(), slot: Slot::SessionId }),
                Piece::Text("--model".into()),
                Piece::Slot(Slot::LaunchModel),
                Piece::Text("--effort".into()),
                Piece::Slot(Slot::LaunchEffort),
                Piece::Slot(Slot::ModuleSwitches),
                Piece::Flagged(Flagged_Data { string: "--disallowedTools".into(), slot: Slot::DeniedTools }),
                Piece::Flagged(Flagged_Data { string: "--tools".into(), slot: Slot::AllowedTools }),
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
