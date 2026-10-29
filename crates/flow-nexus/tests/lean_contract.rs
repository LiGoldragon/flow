use flow_nexus::generated::starter::{LeanStart, RendersLeanStart};
use signal_flow::{HarnessKind, ToolSet};

fn lean(tool_set: ToolSet) -> LeanStart {
    LeanStart {
        harness_kind: HarnessKind::Claude,
        harness_module_vector: vec![],
        left_out: vec![],
        tool_set,
    }
}

#[test]
fn a_declared_tool_set_is_passed_exactly_and_the_default_passes_none() {
    assert_eq!(lean(ToolSet::Default).arguments(), Vec::<String>::new());
    assert_eq!(
        lean(ToolSet::Explicit(vec!["Read".into(), "Bash(*)".into()])).arguments(),
        ["--tools", "Read,Bash(*)"]
    );
    assert_eq!(lean(ToolSet::Explicit(vec![])).arguments(), ["--tools", ""]);
}
