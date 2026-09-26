{
  description = "Typed durable Flow Nexus and clients.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);
      mkContext =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.complete.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-src"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src = craneLib.cleanCargoSource ./.;
          commonArgs = {
            inherit src;
            pname = "flow-workspace";
            version = "0.15.0";
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          exactTest =
            package: testName:
            craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoTestExtraArgs = "-p ${package} ${testName} -- --exact";
              }
            );
        in
        {
          inherit
            pkgs
            toolchain
            craneLib
            commonArgs
            cargoArtifacts
            exactTest
            ;
        };
    in
    {
      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              pname = "flow";
              cargoExtraArgs = "--workspace";
            }
          );
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--workspace";
            }
          );
          fmt = context.craneLib.cargoFmt context.commonArgs;
          clippy = context.craneLib.cargoClippy (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoClippyExtraArgs = "--workspace --all-targets --all-features -- -D warnings";
            }
          );
          flow-v5-row-preservation = context.exactTest "flow-nexus"
            "store::tests::an_existing_v5_row_reopens_unchanged_and_defaults_to_unavailable_route";
          flow-herdr-route-durability = context.exactTest "flow-nexus"
            "tests::running_nexus_parses_actual_working_interactive_snapshot";
          flow-stale-route-unavailable = context.exactTest "flow-nexus"
            "tests::running_nexus_marks_stale_or_noninteractive_snapshots_unavailable";
          flow-conflicting-registration-refusal = context.exactTest "flow-nexus"
            "tests::duplicate_registration_is_idempotent_and_conflict_has_no_partial_mutation";
          flow-herdr-registration-binding = context.exactTest "flow-meta"
            "tests::registration_carries_the_complete_herdr_binding";
          flow-native-resolution-serialization = context.exactTest "flow"
            "tests::native_resolution_serialization_matches_the_signal_contract";
          flow-source-path-either-spelling = context.exactTest "flow-nexus"
            "composition::tests::a_source_inside_the_root_is_read_whether_it_is_written_absolute_or_relative";
          flow-source-outside-root-refused = context.exactTest "flow-nexus"
            "composition::tests::a_source_outside_the_root_is_refused_in_either_spelling";
          flow-gone-pane-lists-exited = context.exactTest "flow-nexus"
            "tests::a_flow_whose_pane_left_herdr_is_listed_exited_and_keeps_its_row";
          flow-unreadable-herdr-changes-nothing = context.exactTest "flow-nexus"
            "tests::an_unreadable_herdr_leaves_a_listed_flow_as_it_stands";
          flow-start-settles-before-answering = context.exactTest "flow-nexus"
            "tests::a_plain_start_answers_started_rather_than_the_transient_ambiguity";
          flow-brief-continuation-after-receipt = context.exactTest "flow-nexus"
            "tests::replace_stops_the_predecessor_before_the_successor_is_routable";
          flow-live-pane-lists-active = context.exactTest "flow-nexus"
            "tests::a_pending_seat_whose_pane_herdr_shows_live_is_listed_active";
          flow-retire-keeps-history = context.exactTest "flow-nexus"
            "tests::retire_keeps_the_row_and_takes_the_flow_out_of_receiving";
          flow-deliver-never-interleaves = context.exactTest "flow-nexus"
            "tests::delivery::two_deliveries_to_one_pane_never_interleave";
          flow-deliver-refuses-commands-and-keys = context.exactTest "flow-nexus"
            "tests::delivery::bodies_carrying_commands_or_keys_are_refused_and_nothing_is_typed";
          flow-deliver-crash-settles-uncertain = context.exactTest "flow-nexus"
            "tests::delivery::a_delivery_a_crash_left_under_its_lease_settles_uncertain_and_is_never_retried";
          flow-hard-abrupt-interrupts-first = context.exactTest "flow-nexus"
            "tests::delivery::hard_abrupt_interrupts_a_working_recipient_before_typing";
          flow-soft-waits-for-rest = context.exactTest "flow-nexus"
            "tests::delivery::soft_waits_for_a_resting_recipient_and_types_nothing_to_a_working_one";
        }
      );

      devShells = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.pkgs.mkShell {
            packages = [
              context.toolchain
              context.pkgs.jujutsu
              context.pkgs.nix
            ];
          };
        }
      );
    };
}
