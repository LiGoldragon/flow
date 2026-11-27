//! Admission of explicit tool catalogs at the last point before a harness
//! effect. Command-line intent is not evidence of the model-visible catalog.

use crate::RunningNexus;
use signal_flow::{
    DeclaredTools, FixedTools, HarnessKind, ToolCatalogObservation, ToolCatalogRefusal, ToolSet,
    Unenforceable_Data,
};

/// Authorizes the model-visible catalog before a launch can contact its
/// provider. A future positive authority must return and durably record a
/// `ToolCatalogReceipt`; launch input itself is never evidence.
pub(crate) trait AuthorizesToolCatalog {
    fn authorize_tool_catalog(
        harness_kind: &HarnessKind,
        tool_set: &ToolSet,
    ) -> Result<(), ToolCatalogRefusal>;
}

impl AuthorizesToolCatalog for RunningNexus {
    fn authorize_tool_catalog(
        harness_kind: &HarnessKind,
        tool_set: &ToolSet,
    ) -> Result<(), ToolCatalogRefusal> {
        let ToolSet::Explicit(declared) = tool_set else {
            return Ok(());
        };
        if harness_kind != &HarnessKind::Claude {
            return Ok(());
        }

        // Stock Claude exposes no authoritative prelaunch catalog, and no
        // tools are configured as unavoidable. Provider-, plugin- and
        // MCP-supplied tools are never fixed tools.
        Err(ToolCatalogRefusal::Unenforceable(Unenforceable_Data {
            harness_kind: harness_kind.clone(),
            tool_catalog_observation: ToolCatalogObservation {
                declared_tools: DeclaredTools(declared.clone()),
                fixed_tools: FixedTools(vec![]),
                observed_tools_option: None,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stock_claude_explicit_catalog_is_unenforceable_without_observation() {
        let declared = vec!["Bash".into(), "Read".into()];
        let refusal = RunningNexus::authorize_tool_catalog(
            &HarnessKind::Claude,
            &ToolSet::Explicit(declared.clone()),
        )
        .unwrap_err();
        assert_eq!(
            refusal,
            ToolCatalogRefusal::Unenforceable(Unenforceable_Data {
                harness_kind: HarnessKind::Claude,
                tool_catalog_observation: ToolCatalogObservation {
                    declared_tools: DeclaredTools(declared),
                    fixed_tools: FixedTools(vec![]),
                    observed_tools_option: None,
                },
            })
        );
    }

    #[test]
    fn defaults_and_other_harnesses_are_unchanged() {
        assert!(
            RunningNexus::authorize_tool_catalog(&HarnessKind::Claude, &ToolSet::Default).is_ok()
        );
        assert!(
            RunningNexus::authorize_tool_catalog(
                &HarnessKind::Codex,
                &ToolSet::Explicit(vec!["Read".into()]),
            )
            .is_ok()
        );
    }
}
