//! Configured harness modules rendered for one launch.

use crate::generated::starter::{LeanStart, RendersLeanStart};
use meta_signal_flow::{ModuleText, Setting, Switch};
use signal_flow::{HarnessKind, ToolSet};

pub(crate) trait ReadsLeanModules {
    fn left_out_module(&self, name: &str) -> bool;
}

impl ReadsLeanModules for LeanStart {
    fn left_out_module(&self, name: &str) -> bool {
        self.left_out.iter().any(|left| left == name)
    }
}

impl RendersLeanStart for LeanStart {
    fn arguments(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for module in &self.harness_module_vector {
            if !self.left_out_module(&module.harness_module_name) {
                continue;
            }
            for switch in &module.switches {
                if let Switch::Flag(words) = switch {
                    arguments.extend(words.iter().cloned());
                }
            }
        }
        let tools = self.disallowed_tools();
        if !tools.is_empty() {
            arguments.push("--disallowedTools".into());
            arguments.push(tools.join(","));
        }
        if self.harness_kind == HarnessKind::Claude {
            if let ToolSet::Explicit(tools) = &self.tool_set {
                arguments.push("--tools".into());
                arguments.push(tools.join(","));
            }
        }
        arguments
    }

    fn environment(&self) -> Vec<String> {
        let mut out = Vec::new();
        for module in &self.harness_module_vector {
            if !self.left_out_module(&module.harness_module_name) {
                continue;
            }
            for switch in &module.switches {
                if let Switch::Environment(assignment) = switch {
                    let value = format!(
                        "{}={}",
                        assignment.environment_name, assignment.environment_value
                    );
                    if !out.contains(&value) {
                        out.push(value);
                    }
                }
            }
        }
        out
    }

    fn unset_environment(&self) -> Vec<String> {
        let mut out = Vec::new();
        for module in &self.harness_module_vector {
            if self.left_out_module(&module.harness_module_name) {
                continue;
            }
            for switch in &module.switches {
                if let Switch::Environment(assignment) = switch {
                    if !out.contains(&assignment.environment_name) {
                        out.push(assignment.environment_name.clone());
                    }
                }
            }
        }
        out
    }

    fn settings(&self) -> Vec<Setting> {
        let mut out = Vec::new();
        for module in &self.harness_module_vector {
            if !self.left_out_module(&module.harness_module_name) {
                continue;
            }
            for switch in &module.switches {
                if let Switch::Setting(setting) = switch {
                    if !out
                        .iter()
                        .any(|known: &Setting| known.setting_key == setting.setting_key)
                    {
                        out.push(setting.clone());
                    }
                }
            }
        }
        out
    }

    fn disallowed_tools(&self) -> Vec<String> {
        let mut out = Vec::new();
        for module in &self.harness_module_vector {
            if !self.left_out_module(&module.harness_module_name) {
                continue;
            }
            for tool in &module.tools {
                if !out.contains(tool) {
                    out.push(tool.clone());
                }
            }
        }
        out
    }

    fn attribution_off(&self) -> bool {
        self.settings()
            .iter()
            .any(|setting| setting.setting_key == "attribution")
    }

    fn skill_listing_off(&self) -> bool {
        self.harness_module_vector.iter().any(|module| {
            self.left_out_module(&module.harness_module_name)
                && (module.harness_module_name == "Skill" || matches!(module.module_text, ModuleText::Retained(ref text) if text.contains("<skills_instructions>")))
        })
    }
}
