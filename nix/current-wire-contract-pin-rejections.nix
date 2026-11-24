# A current wire contract is fixed by its full Git revision in the workspace
# declaration and the identical resolved source in Cargo.lock.
{ manifest, lock }:
let
  declared = manifest.workspace.dependencies;
  matchesLock =
    name:
    let
      dependency = declared.${name} or { };
      revision = dependency.rev or "";
      repository = dependency.git or "";
      expectedSource = "git+${repository}?rev=${revision}#${revision}";
    in
    repository != ""
    && builtins.match "[0-9a-f]{40}" revision != null
    && builtins.any (package: package.name == name && (package.source or "") == expectedSource) lock.package;
in
builtins.filter (name: !matchesLock name) [
  "signal-flow"
  "meta-signal-flow"
]
