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
          # The Operation root's ethos source is read by flow-nexus's build
          # script, which holds the committed generated module fresh.
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            name = "source";
            filter =
              path: type:
              (pkgs.lib.hasSuffix ".ethos" path) || (craneLib.filterCargoSources path type);
          };
          commonArgs = {
            inherit src;
            pname = "flow-workspace";
            version = "0.22.0";
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          # The two trait laws are read off the Rust text, so their check
          # needs every `.rs` file plus the shell the check itself is written
          # in — a different set from what crane compiles.
          lawSource = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (type == "directory")
              || (type == "regular" && pkgs.lib.hasSuffix ".rs" path)
              || (type == "regular" && pkgs.lib.hasSuffix ".sh" path);
          };
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
            lawSource
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
          # `fn main()` is the only production free function, and every
          # production method lives in a trait.
          no-free-functions =
            context.pkgs.runCommand "flow-no-free-functions" { src = context.lawSource; }
              (builtins.readFile ./checks/no-free-functions.sh);
          no-inherent-methods =
            context.pkgs.runCommand "flow-no-inherent-methods" { src = context.lawSource; }
              (builtins.readFile ./checks/no-inherent-methods.sh);
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
          flow-operation-outcomes = context.exactTest "flow-nexus"
            "tests::an_operation_is_answered_by_its_own_outcome";
          flow-report-recorded-or-refused = context.exactTest "flow-nexus"
            "tests::a_report_is_recorded_for_a_held_flow_and_refused_for_an_unknown_one";
          flow-events-kept-across-reopen = context.exactTest "flow-nexus"
            "store::events::tests::a_flow_keeps_its_reported_events_in_order_across_a_reopen";
          flow-hook-reports-each-event = context.exactTest "flow"
            "tests::each_harness_event_becomes_one_report_of_the_claimed_flow";
          flow-reserve-holds-the-flow-id = context.exactTest "flow-nexus"
            "tests::reservation::a_claude_launch_holds_its_flow_id_from_reserve_before_any_harness";
          flow-reserved-flow-id-reaches-the-harness = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_reserved_claude_launch_starts_as_its_session_with_its_flow_id";
          flow-reserved-flow-id-in-the-environment = context.exactTest "flow-nexus"
            "herdr::launch::tests::claude_environment_preparation_exports_the_reserved_flow_id";
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
          flow-claude-receipt-without-effort = context.exactTest "flow-nexus"
            "herdr::launch::tests::claude_receipt_of_a_model_without_effort_is_observed";
          flow-hard-abrupt-presses-again = context.exactTest "flow-nexus"
            "tests::delivery::hard_abrupt_presses_the_interrupt_again_while_the_recipient_still_works";
          flow-hard-abrupt-bounded-presses = context.exactTest "flow-nexus"
            "tests::delivery::an_interrupt_that_never_shows_is_pressed_a_bounded_number_of_times";
          flow-exited-refusal-names-exited = context.exactTest "flow-nexus"
            "tests::a_flow_whose_pane_left_herdr_is_listed_exited_and_keeps_its_row";
          flow-claude-retract-is-one-ctrl-c = context.exactTest "flow-nexus"
            "tests::submission::a_letter_claudes_interrupt_put_back_is_taken_out_by_one_ctrl_c";
          flow-claude-draft-is-never-retracted = context.exactTest "flow-nexus"
            "tests::submission::a_draft_claudes_interrupt_put_back_is_never_taken_out";
          flow-caller-pane-from-marks-or-ancestry = context.exactTest "flow-nexus"
            "tests::a_process_in_a_pane_is_found_by_its_own_marks_or_its_ancestors";
          flow-client-default-socket = context.exactTest "flow"
            "the_client_reaches_the_default_ordinary_socket_under_the_runtime_directory";
          flow-meta-client-default-socket = context.exactTest "flow-meta"
            "the_meta_client_reaches_the_default_meta_socket_under_the_runtime_directory";
          flow-configuration-only-over-meta = context.exactTest "flow-nexus"
            "a_flow_variable_in_the_environment_does_not_reach_the_configuration";
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
