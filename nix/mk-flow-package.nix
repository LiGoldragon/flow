# Build a Flow package whose executable carries the configuration used only
# when it opens a new, unconfigured Memory.  The generated Rust is compiled
# with Flow; a malformed value therefore fails before a package exists.
#
# Only flow-nexus's daemon binary is compiled again, with the
# executable-default-configuration feature and the generated module, against
# the one flow-nexus library every build links; every other binary and
# member is its ordinary build, and no test is compiled again.  The package
# exists only once every member's check has passed and the configured daemon
# has seeded a fresh memory with what it carries.
{
  lib,
  pkgs,
  # The flow-nexus member build, as the build base makes it.
  flowNexus,
  # The other members' builds, whose binaries the package carries.
  members,
  # Every member's check, flow-nexus's among them, which the package waits on.
  memberChecks,
  # Runs one ignored test of the flow-nexus test build: name, test name,
  # the shell that sets its environment.
  processCheck,
  # Copies the binaries of builds into one bin directory.
  binaries,
  version,
}:
{
  defaultConfiguration,
}:
let
  exactAttrs =
    name: wanted: value:
    let
      actual = builtins.attrNames value;
      missing = builtins.filter (field: !(builtins.hasAttr field value)) wanted;
      extra = builtins.filter (field: !(builtins.elem field wanted)) actual;
    in
    if !builtins.isAttrs value then
      throw "${name} must be an attribute set"
    else if missing != [ ] then
      throw "${name} is missing required fields: ${lib.concatStringsSep ", " missing}"
    else if extra != [ ] then
      throw "${name} has unsupported fields: ${lib.concatStringsSep ", " extra}"
    else
      value;

  requireString =
    name: value:
    if builtins.isString value && value != "" then
      value
    else
      throw "${name} must be a non-empty string";

  requireAbsolute =
    name: value:
    let
      checked = requireString name value;
    in
    if lib.hasPrefix "/" checked then checked else throw "${name} must be an absolute path";

  requireStringList =
    name: value:
    if !(builtins.isList value) then
      throw "${name} must be a list"
    else if builtins.any (item: !(builtins.isString item) || item == "") value then
      throw "${name} must contain only non-empty strings"
    else
      value;

  endpoint =
    name: value:
    let
      fields = exactAttrs name [ "clientPath" "home" "controlSocketPath" "roster" ] value;
    in
    {
      clientPath = requireAbsolute "${name}.clientPath" fields.clientPath;
      home = requireAbsolute "${name}.home" fields.home;
      controlSocketPath = requireAbsolute "${name}.controlSocketPath" fields.controlSocketPath;
      roster = requireStringList "${name}.roster" fields.roster;
    };

  harnessProfile =
    index: value:
    let
      name = "defaultConfiguration.harnessProfiles[${toString index}]";
      fields = exactAttrs name [ "harnessKind" "commandSigils" "interruptKeys" "submitKeys" ] value;
      kind = requireString "${name}.harnessKind" fields.harnessKind;
    in
    if
      !(builtins.elem kind [
        "Codex"
        "Claude"
      ])
    then
      throw "${name}.harnessKind must be Codex or Claude"
    else
      {
        harnessKind = kind;
        commandSigils = requireStringList "${name}.commandSigils" fields.commandSigils;
        interruptKeys = requireStringList "${name}.interruptKeys" fields.interruptKeys;
        submitKeys = requireStringList "${name}.submitKeys" fields.submitKeys;
      };

  configurationFields = exactAttrs "defaultConfiguration" [
    "sourceRoot"
    "runtimeDirectory"
    "stableCodex"
    "nextCodex"
    "harnessProfiles"
    "metaAspects"
    "presentationPrompt"
    "herdrSession"
  ] defaultConfiguration;

  stableCodex = endpoint "defaultConfiguration.stableCodex" configurationFields.stableCodex;
  nextCodex = endpoint "defaultConfiguration.nextCodex" configurationFields.nextCodex;
  sourceRoot = requireAbsolute "defaultConfiguration.sourceRoot" configurationFields.sourceRoot;
  runtimeDirectory = requireAbsolute "defaultConfiguration.runtimeDirectory" configurationFields.runtimeDirectory;
  harnessProfiles =
    if
      !(builtins.isList configurationFields.harnessProfiles) || configurationFields.harnessProfiles == [ ]
    then
      throw "defaultConfiguration.harnessProfiles must be a non-empty list"
    else
      lib.imap0 harnessProfile configurationFields.harnessProfiles;
  metaAspects = requireStringList "defaultConfiguration.metaAspects" configurationFields.metaAspects;
  # The one Herdr session every launch opens its pane in.
  herdrSession =
    if builtins.isString configurationFields.herdrSession && configurationFields.herdrSession != "" then
      configurationFields.herdrSession
    else
      throw "defaultConfiguration.herdrSession must be a non-empty string";
  presentationPrompt =
    if configurationFields.presentationPrompt == null then
      null
    else if builtins.isString configurationFields.presentationPrompt then
      configurationFields.presentationPrompt
    else
      throw "defaultConfiguration.presentationPrompt must be null or a string";
  unsupportedAspects = builtins.filter (
    aspect:
    !(builtins.elem aspect [
      "Psyche"
      "Mind"
      "Field"
    ])
  ) metaAspects;
  overlappingModels = builtins.filter (
    model: builtins.elem model nextCodex.roster
  ) stableCodex.roster;

  quote = builtins.toJSON;
  # A vector of one newtype over String, each value wrapped by its type.
  rustStrings =
    newtype: values:
    "vec![${lib.concatMapStringsSep ", " (value: "${newtype}(${quote value}.to_owned())") values}]";
  rustEndpoint = value: ''
    flow_ethos::CodexEndpoint {
        client_path: flow_ethos::ClientPath(${quote value.clientPath}.to_owned()),
        home: flow_ethos::Home(${quote value.home}.to_owned()),
        control_socket_path: flow_ethos::ControlSocketPath(${quote value.controlSocketPath}.to_owned()),
        model_name_vector: ${rustStrings "flow_ethos::ModelName" value.roster},
    }'';
  rustProfile = value: ''
    flow_ethos::HarnessProfile {
        harness_kind: flow_ethos::HarnessKind::${value.harnessKind},
        command_sigil_vector: ${rustStrings "flow_ethos::CommandSigil" value.commandSigils},
        interrupt_keys: flow_ethos::InterruptKeys(${rustStrings "flow_ethos::KeyName" value.interruptKeys}),
        submit_keys: flow_ethos::SubmitKeys(${rustStrings "flow_ethos::KeyName" value.submitKeys}),
    }'';
  rustAspect = aspect: "flow_ethos::Aspect::${aspect}";

  curriculumNexusPath = "${runtimeDirectory}/curriculum/curriculum.sock";
  generatedModule = pkgs.writeText "flow-default-configuration.rs" ''
    // Generated by nix/mk-flow-package.nix.  It is Rust, rather than an
    // untyped runtime file, so contract drift is a build failure.
    pub(crate) fn executable_default_configuration() -> meta_signal_flow::ConfigureNexus {
        meta_signal_flow::ConfigureNexus {
            source_root: flow_ethos::SourceRoot(${quote sourceRoot}.to_owned()),
            // The stable Codex endpoint first, the next one second.
            first_codex_endpoint: ${rustEndpoint stableCodex},
            second_codex_endpoint: ${rustEndpoint nextCodex},
            harness_profiles: flow_ethos::HarnessProfiles(vec![
                ${lib.concatMapStringsSep ",\n        " rustProfile harnessProfiles}
            ]),
            meta_aspects: flow_ethos::MetaAspects(vec![${
              lib.concatMapStringsSep ", " rustAspect metaAspects
            }]),
            message_nexus_path_option: None,
            message_nexus_binary_option: None,
            curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(${quote curriculumNexusPath}.to_owned()),
            lease: flow_ethos::Lease(60),
            presentation_prompt_option: ${
              if presentationPrompt == null then
                "None"
              else
                "Some(meta_signal_flow::PresentationPrompt(${quote presentationPrompt}.to_owned()))"
            },
            herdr_session_name: signal_flow::HerdrSessionName(${quote herdrSession}.to_owned()),
        }
    }
  '';

  validated =
    if stableCodex.roster == [ ] then
      throw "defaultConfiguration.stableCodex.roster must not be empty"
    else if nextCodex.roster == [ ] then
      throw "defaultConfiguration.nextCodex.roster must not be empty"
    else if overlappingModels != [ ] then
      throw "stableCodex and nextCodex rosters overlap: ${lib.concatStringsSep ", " overlappingModels}"
    else if unsupportedAspects != [ ] then
      throw "defaultConfiguration.metaAspects contains unknown aspects: ${lib.concatStringsSep ", " unsupportedAspects}"
    else
      true;
in
assert validated;
let
  configured = "executable-default-configuration";
  # The generated module reaches flow-nexus's rustc as the variable its
  # include! names.
  withModule =
    overrides:
    overrides
    // {
      flow-nexus =
        attributes:
        (overrides.flow-nexus or (_: { })) attributes
        // {
          FLOW_NEXUS_DEFAULT_CONFIGURATION_RS = generatedModule;
        };
    };
  daemon = "flow-nexus-daemon";
  # The feature is read by the daemon's main alone, so the daemon is
  # compiled again, linking the library the unconfigured build made.
  configuredDaemon = flowNexus.overrideBinaries (old: {
    features = old.features ++ [ configured ];
    crateOverrides = withModule old.crateOverrides;
    crateBin = lib.filter (binary: binary.name == daemon) old.crateBin;
  });
  # flow-nexus's other binaries, as its build makes them.
  otherNexusBinaries = pkgs.runCommand "flow-nexus-${version}-unconfigured-binaries" { } ''
    mkdir -p "$out/bin"
    for binary in ${flowNexus}/bin/*; do
      if [ "$(basename "$binary")" != ${daemon} ]; then
        ln -s "$binary" "$out/bin/"
      fi
    done
  '';
  configuredNexusCheck =
    processCheck "configured-daemon-seeds" "configured_executable_seeds_only_its_fresh_typed_nexus_row"
      ''
        export FLOW_NEXUS_CONFIGURED_DAEMON=${configuredDaemon}/bin/${daemon}
      '';
in
binaries "flow-configured-${version}"
  (
    [
      configuredDaemon
      otherNexusBinaries
    ]
    ++ members
  )
  {
    checked = [ configuredNexusCheck ] ++ memberChecks;
    passthru = {
      inherit generatedModule configuredNexusCheck;
      configured = true;
    };
  }
