//! A fail-closed compiler from an observed Claude tool-definition catalog to
//! the role's one shared tool manifest.
//!
//! The compiler is deliberately independent of an Address.  All birth,
//! restart and direct adapters can consume the same receipt and Claude
//! arguments.  It accepts no inferred catalog: the definitions supplied by
//! the adapter must match the attested Claude 2.1.331 catalog exactly.

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, io::Write, path::Path};

pub const POLICY_NAME: &str = "DesignerToolManifestV0";
pub const CATALOG_SHA256: &str = "b3b657cd1d024acdaf18dab3caf4420ad58c086473d3110eb1998726de3c0b12";
pub const CATALOG_BYTES: usize = 66_637;
pub const DESIGNER_CATALOG_SHA256: &str =
    "e7aa275c4165c96ba4f492cf34c99cb45cfc5996b0844c12e3463cf4158321e5";
pub const DESIGNER_CATALOG_BYTES: usize = 23_655;
pub const CATALOG_NAMES: [&str; 13] = [
    "Agent",
    "Artifact",
    "Bash",
    "Edit",
    "Read",
    "ReportFindings",
    "ScheduleWakeup",
    "SendFeedback",
    "Skill",
    "ToolSearch",
    "DeferredToolPlaceholder",
    "Write",
    "advisor",
];
pub const DESIGNER_NAMES: [&str; 11] = [
    "Bash",
    "Edit",
    "Read",
    "ReportFindings",
    "ScheduleWakeup",
    "SendFeedback",
    "Skill",
    "ToolSearch",
    "DeferredToolPlaceholder",
    "Write",
    "advisor",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersonaRole {
    Designer,
    Publisher,
    Other,
}

impl PersonaRole {
    pub fn parse(text: &str) -> Result<Self, RoleToolManifestRefusal> {
        match text {
            "Designer" => Ok(Self::Designer),
            "Publisher" => Ok(Self::Publisher),
            "Other" => Ok(Self::Other),
            _ => Err(RoleToolManifestRefusal::UnknownRole(text.to_owned())),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Designer => "Designer",
            Self::Publisher => "Publisher",
            Self::Other => "Other",
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RoleToolManifestRefusal {
    #[error("unknown role {0}")]
    UnknownRole(String),
    #[error("tool catalog is not a JSON vector")]
    CatalogNotVector,
    #[error("tool definition {0} has no string name")]
    DefinitionUnnamed(usize),
    #[error("tool definition name occurs twice: {0}")]
    DuplicateDefinition(String),
    #[error("tool catalog names differ from the attested catalog")]
    CatalogNamesDiffer,
    #[error("tool catalog bytes differ: observed {observed}, expected {expected}")]
    CatalogBytesDiffer { observed: usize, expected: usize },
    #[error("tool catalog digest differs: observed {observed}, expected {expected}")]
    CatalogDigestDiffer {
        observed: String,
        expected: &'static str,
    },
    #[error("compiled role catalog names differ from policy")]
    CompiledNamesDiffer,
    #[error("compiled role catalog bytes differ: observed {observed}, expected {expected}")]
    CompiledBytesDiffer { observed: usize, expected: usize },
    #[error("compiled role catalog digest differs: observed {observed}, expected {expected}")]
    CompiledDigestDiffer {
        observed: String,
        expected: &'static str,
    },
    #[error("catalog input could not be read: {0}")]
    Read(String),
    #[error("catalog input is not JSON: {0}")]
    Json(String),
    #[error("manifest receipt already exists or could not be recorded: {0}")]
    Record(String),
    #[error("harness arguments already carry a tool-catalog option: {0}")]
    ConflictingToolArgument(String),
}

#[derive(Debug)]
pub struct CompiledRoleToolManifest {
    pub receipt: Value,
    pub filtered_catalog: Value,
    pub claude_arguments: Vec<String>,
}

/// Applies the verified manifest to the exact argument vector handed to the
/// Claude recorder/spawner.  A caller cannot layer this policy over another
/// tool selection: that would make the recorded manifest cease to describe
/// the actual command line.
pub fn integrate_manifest_arguments(
    harness_arguments: &[String],
    manifest: &CompiledRoleToolManifest,
) -> Result<Vec<String>, RoleToolManifestRefusal> {
    for argument in harness_arguments {
        if matches!(
            argument.as_str(),
            "--tools" | "--disallowedTools" | "--allowedTools"
        ) || argument.starts_with("--tools=")
            || argument.starts_with("--disallowedTools=")
            || argument.starts_with("--allowedTools=")
        {
            return Err(RoleToolManifestRefusal::ConflictingToolArgument(
                argument.clone(),
            ));
        }
    }
    let mut integrated = harness_arguments.to_vec();
    integrated.extend(manifest.claude_arguments.iter().cloned());
    Ok(integrated)
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        Value::Object(fields) => {
            let mut names: Vec<_> = fields.keys().collect();
            names.sort();
            let mut sorted = Map::new();
            for name in names {
                sorted.insert(name.clone(), canonical(&fields[name]));
            }
            Value::Object(sorted)
        }
        value => value.clone(),
    }
}

fn encoded(value: &Value) -> Vec<u8> {
    serde_json::to_vec(&canonical(value)).expect("a JSON value always serializes")
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn names(definitions: &[Value]) -> Result<Vec<String>, RoleToolManifestRefusal> {
    let mut seen = BTreeSet::new();
    let mut names = Vec::with_capacity(definitions.len());
    for (index, definition) in definitions.iter().enumerate() {
        let name = definition
            .get("name")
            .and_then(Value::as_str)
            .ok_or(RoleToolManifestRefusal::DefinitionUnnamed(index))?;
        if !seen.insert(name.to_owned()) {
            return Err(RoleToolManifestRefusal::DuplicateDefinition(
                name.to_owned(),
            ));
        }
        names.push(name.to_owned());
    }
    Ok(names)
}

fn matches_names(actual: &[String], expected: &[&str]) -> bool {
    let actual: BTreeSet<_> = actual.iter().map(String::as_str).collect();
    let expected: BTreeSet<_> = expected.iter().copied().collect();
    actual == expected
}

pub fn compile_role_tool_manifest(
    role: PersonaRole,
    catalog: Value,
) -> Result<CompiledRoleToolManifest, RoleToolManifestRefusal> {
    let definitions = catalog
        .as_array()
        .ok_or(RoleToolManifestRefusal::CatalogNotVector)?;
    let catalog_names = names(definitions)?;
    if !matches_names(&catalog_names, &CATALOG_NAMES) {
        return Err(RoleToolManifestRefusal::CatalogNamesDiffer);
    }
    let catalog_encoded = encoded(&catalog);
    if catalog_encoded.len() != CATALOG_BYTES {
        return Err(RoleToolManifestRefusal::CatalogBytesDiffer {
            observed: catalog_encoded.len(),
            expected: CATALOG_BYTES,
        });
    }
    let catalog_digest = digest(&catalog_encoded);
    if catalog_digest != CATALOG_SHA256 {
        return Err(RoleToolManifestRefusal::CatalogDigestDiffer {
            observed: catalog_digest,
            expected: CATALOG_SHA256,
        });
    }

    let (filtered, expected_names, expected_bytes, expected_digest) = match role {
        PersonaRole::Designer => (
            definitions
                .iter()
                .filter(|definition| {
                    !matches!(
                        definition.get("name").and_then(Value::as_str),
                        Some("Agent" | "Artifact")
                    )
                })
                .cloned()
                .collect::<Vec<_>>(),
            DESIGNER_NAMES.as_slice(),
            DESIGNER_CATALOG_BYTES,
            DESIGNER_CATALOG_SHA256,
        ),
        PersonaRole::Publisher | PersonaRole::Other => (
            definitions.clone(),
            CATALOG_NAMES.as_slice(),
            CATALOG_BYTES,
            CATALOG_SHA256,
        ),
    };
    let filtered_names = names(&filtered)?;
    if !matches_names(&filtered_names, expected_names) {
        return Err(RoleToolManifestRefusal::CompiledNamesDiffer);
    }
    let filtered_catalog = Value::Array(filtered);
    let filtered_encoded = encoded(&filtered_catalog);
    if filtered_encoded.len() != expected_bytes {
        return Err(RoleToolManifestRefusal::CompiledBytesDiffer {
            observed: filtered_encoded.len(),
            expected: expected_bytes,
        });
    }
    let filtered_digest = digest(&filtered_encoded);
    if filtered_digest != expected_digest {
        return Err(RoleToolManifestRefusal::CompiledDigestDiffer {
            observed: filtered_digest,
            expected: expected_digest,
        });
    }
    let claude_arguments = vec!["--tools".to_owned(), filtered_names.join(",")];
    let receipt = json!({
        "policy": POLICY_NAME,
        "role": role.name(),
        "catalog": { "sha256": CATALOG_SHA256, "bytes": CATALOG_BYTES, "definitions": CATALOG_NAMES.len() },
        "compiled": { "sha256": expected_digest, "bytes": expected_bytes, "definitions": expected_names.len(), "names": filtered_names },
        "claude_arguments": claude_arguments,
        "claim": "canonical JSON reserialization and selected CLI arguments; not a wire, token, context, or model-visible inventory receipt"
    });
    Ok(CompiledRoleToolManifest {
        receipt,
        filtered_catalog,
        claude_arguments,
    })
}

pub fn compile_file_and_record(
    role: PersonaRole,
    catalog_path: &Path,
    receipt_path: &Path,
) -> Result<CompiledRoleToolManifest, RoleToolManifestRefusal> {
    let bytes =
        fs::read(catalog_path).map_err(|error| RoleToolManifestRefusal::Read(error.to_string()))?;
    let catalog = serde_json::from_slice(&bytes)
        .map_err(|error| RoleToolManifestRefusal::Json(error.to_string()))?;
    let compiled = compile_role_tool_manifest(role, catalog)?;
    let mut receipt = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(receipt_path)
        .map_err(|error| RoleToolManifestRefusal::Record(error.to_string()))?;
    let receipt_bytes = serde_json::to_vec_pretty(&compiled.receipt)
        .map_err(|error| RoleToolManifestRefusal::Record(error.to_string()))?;
    receipt
        .write_all(&receipt_bytes)
        .and_then(|_| receipt.write_all(b"\n"))
        .and_then(|_| receipt.sync_all())
        .map_err(|error| RoleToolManifestRefusal::Record(error.to_string()))?;
    Ok(compiled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_attestation_constants_are_internally_consistent() {
        assert_eq!(CATALOG_NAMES.len(), 13);
        assert_eq!(DESIGNER_NAMES.len(), 11);
        assert!(!DESIGNER_NAMES.contains(&"Agent"));
        assert!(!DESIGNER_NAMES.contains(&"Artifact"));
        assert!(CATALOG_NAMES.contains(&"Agent"));
        assert!(CATALOG_NAMES.contains(&"Artifact"));
        assert_eq!(CATALOG_BYTES - DESIGNER_CATALOG_BYTES, 42_982);
    }

    #[test]
    fn unknown_role_refuses() {
        assert_eq!(
            PersonaRole::parse("Editor"),
            Err(RoleToolManifestRefusal::UnknownRole("Editor".into()))
        );
    }

    #[test]
    fn malformed_catalog_refuses_before_a_manifest_exists() {
        assert_eq!(
            compile_role_tool_manifest(PersonaRole::Designer, json!({})).unwrap_err(),
            RoleToolManifestRefusal::CatalogNotVector
        );
        let wrong = json!([{ "name": "Agent" }, { "name": "Agent" }]);
        assert_eq!(
            compile_role_tool_manifest(PersonaRole::Designer, wrong).unwrap_err(),
            RoleToolManifestRefusal::DuplicateDefinition("Agent".into())
        );
    }

    #[test]
    fn command_line_integration_refuses_a_second_tool_policy() {
        let manifest = CompiledRoleToolManifest {
            receipt: json!({}),
            filtered_catalog: json!([]),
            claude_arguments: vec!["--tools".into(), "Read,Write".into()],
        };
        assert_eq!(
            integrate_manifest_arguments(&["--tools=Read".into()], &manifest),
            Err(RoleToolManifestRefusal::ConflictingToolArgument(
                "--tools=Read".into()
            ))
        );
        assert_eq!(
            integrate_manifest_arguments(&["--model".into(), "claude-fable-5-2".into()], &manifest)
                .unwrap(),
            ["--model", "claude-fable-5-2", "--tools", "Read,Write"]
                .map(String::from)
                .to_vec()
        );
    }
}
