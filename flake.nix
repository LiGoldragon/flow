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
            version = "0.35.18";
            strictDeps = true;
          };
          flowEthosSource = pkgs.lib.cleanSourceWith {
            src = ./crates/flow-ethos;
            filter =
              path: type:
              (type == "directory")
              || (pkgs.lib.hasSuffix ".rs" path)
              || (pkgs.lib.hasSuffix ".ethos" path);
          };
          dependencyDummySrc = craneLib.mkDummySrc (
            commonArgs
            // {
              # The git-pinned Signal and Meta crates depend on this workspace
              # package through the Cargo patch. Crane stubs workspace source
              # in buildDepsOnly, so restore only the shared type crate's real
              # Rust and Ethos inputs after stubbing. Keep its build script
              # dummy so this derivation still only primes dependency artifacts.
              extraDummyScript = ''
                # Crane's stub files are copied from the read-only Nix store.
                # Replace those two dummy trees before restoring the real leaf
                # inputs; never change permissions on the source or store.
                rm -rf "$out/crates/flow-ethos/src" "$out/crates/flow-ethos/ethos"
                mkdir -p "$out/crates/flow-ethos/src" "$out/crates/flow-ethos/ethos"
                cp -R ${flowEthosSource}/src/. "$out/crates/flow-ethos/src/"
                cp -R ${flowEthosSource}/ethos/. "$out/crates/flow-ethos/ethos/"
              '';
            }
          );
          cargoArtifacts = craneLib.buildDepsOnly (
            commonArgs // { dummySrc = dependencyDummySrc; }
          );
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
          # One named test, run exactly once: a name that matches no test
          # (or more than one) fails the check rather than passing empty.
          exactTest =
            package: testName:
            craneLib.mkCargoDerivation (
              commonArgs
              // {
                inherit cargoArtifacts;
                pnameSuffix = "-test";
                doCheck = true;
                buildPhaseCargoCommand = "";
                checkPhaseCargoCommand = ''
                  cargoWithProfile test --locked -p ${package} ${testName} -- --exact 2>&1 | tee exact-test.log
                  status=''${PIPESTATUS[0]}
                  if [ "$status" -ne 0 ]; then
                    exit "$status"
                  fi
                  ran=$(grep -cFx "test ${testName} ... ok" exact-test.log || true)
                  if [ "$ran" -ne 1 ]; then
                    echo "exact test ${package} ${testName} ran $ran times; it must run once" >&2
                    exit 1
                  fi
                '';
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
          # The flow-nexus library test executable, built so a test kept
          # out of every check (one that needs a store no repository holds,
          # named at run time) can be run by name where a build runs.
          store-proof = context.craneLib.mkCargoDerivation (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              pnameSuffix = "-store-proof";
              nativeBuildInputs = [ context.pkgs.jq ];
              buildPhaseCargoCommand = ''
                cargoWithProfile test --locked -p flow-nexus --lib --no-run \
                  --message-format json-render-diagnostics > cargo-test.json
              '';
              installPhaseCommand = ''
                executable=$(jq -r 'select(.reason == "compiler-artifact" and .profile.test and .target.name == "flow_nexus") | .executable' cargo-test.json)
                mkdir -p "$out/bin"
                cp "$executable" "$out/bin/flow-nexus-store-proof"
              '';
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
          flow-v5-flow-row-upgrades-to-derived-origin = context.exactTest "flow-nexus"
            "store::tests::a_v5_flow_row_upgrades_to_its_derived_origin";
          flow-v2-outcomes-upgrade-to-derived-origin = context.exactTest "flow-nexus"
            "store::tests::v2_launch_outcomes_upgrade_to_their_derived_origin";
          flow-0-34-launch-attempt-carried = context.exactTest "flow-nexus"
            "store::tests::a_0_34_launch_attempt_row_is_carried_with_what_it_named_kept";
          flow-codex-thread-opened-lean-before-attach = context.exactTest "flow-nexus"
            "codex::tests::a_codex_thread_is_opened_lean_and_given_its_first_turn_before_attach";
          flow-codex-loaded-instructions-get-no-turn = context.exactTest "flow-nexus"
            "codex::tests::a_codex_thread_that_loaded_left_out_instructions_gets_no_turn";
          flow-codex-exclusions-reapplied-after-attach = context.exactTest "flow-nexus"
            "codex::tests::after_attach_the_exclusions_are_applied_again_and_read_back";
          flow-codex-attached-readback-refused = context.exactTest "flow-nexus"
            "codex::tests::an_attached_thread_whose_readback_differs_is_refused";
          flow-codex-first-turn-before-attach-observed = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_codex_first_turn_given_before_the_tui_attached_is_observed";
          flow-codex-left-out-block-refused = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_codex_request_carrying_a_left_out_block_is_refused";
          flow-codex-tui-attaches-to-opened-thread = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_codex_tui_attaches_to_the_opened_thread_and_binds_only_to_it";
          flow-codex-remote-resumed-tui-binds-opened-thread = context.exactTest "flow-nexus"
            "codex::tests::a_remote_resumed_tui_binds_to_the_thread_the_app_server_opened";
          flow-retired-tables-dropped-on-open = context.exactTest "flow-nexus"
            "store::tests::a_store_from_0_34_6_drops_its_retired_tables_when_it_opens";
          flow-start-origin-derived-from-caller = context.exactTest "flow-nexus"
            "tests::a_start_origin_is_the_bound_flow_in_the_callers_pane_or_unbound";
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
          flow-start-names-only-module-identities = context.exactTest "flow-nexus"
            "composition::tests::a_profile_naming_no_valid_identities_is_refused_before_composition";
          flow-context-verification-refusals = context.exactTest "flow-nexus"
            "context::tests::a_module_body_that_differs_from_its_rendered_hash_is_refused";
          flow-message-split-where-modules-begin = context.exactTest "flow-nexus"
            "composition::tests::a_message_too_large_for_one_part_is_split_where_modules_begin";
          flow-one-message-through-herdr-socket = context.exactTest "flow-nexus"
            "herdr::launch::tests::stages_pane_native_claim_registration_and_one_ambiguous_prompt_write";
          flow-parts-received-in-order = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_two_part_claude_message_is_received_part_by_part_in_order";
          flow-parts-advance-one-at-a-time = context.exactTest "flow-nexus"
            "store::tests::parts_advance_one_at_a_time_and_the_receipt_waits_for_the_last";
          flow-fresh-wake-composes-last-launch-context = context.exactTest "flow-nexus"
            "tests::waking::a_fresh_wake_composes_the_last_launch_modules_and_the_queued_work";
          flow-leave-outs-refused-when-unsafe = context.exactTest "flow-nexus"
            "lean::tests::indispensable_parts_skills_with_workflow_and_other_harness_parts_are_refused";
          flow-user-settings-leave-out-refused = context.exactTest "flow-nexus"
            "lean::tests::user_settings_are_indispensable_and_the_agent_tool_is_not";
          flow-leave-outs-reach-the-harness = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_claude_start_carries_its_leave_outs_as_flags_environment_and_settings";
          flow-start-leave-outs-replace-the-default = context.exactTest "flow-nexus"
            "composition::tests::a_start_naming_leave_outs_replaces_its_metaflow_default";
          flow-leave-out-default-for-another-harness-refused = context.exactTest "flow-nexus"
            "tests::configuring::a_leave_out_default_naming_another_harness_is_refused_and_writes_nothing";
          flow-codex-start-takes-no-claude-default = context.exactTest "flow-nexus"
            "tests::configuring::a_codex_start_naming_none_takes_no_claude_default";
          flow-codex-start-claude-leave-out-refused-before-reserve = context.exactTest "flow-nexus"
            "tests::reservation::a_codex_start_naming_a_claude_leave_out_is_refused_before_reserve_or_open";
          flow-leave-out-defaults-upgrade-under-their-harness = context.exactTest "flow-nexus"
            "store::tests::production_leave_out_defaults_upgrade_each_under_its_harness";
          flow-configuration-only-upgrades-leave-out-defaults = context.exactTest "flow-nexus"
            "store::tests::the_configuration_only_open_upgrades_v1_leave_out_defaults";
          flow-mixed-leave-out-default-refuses-upgrade = context.exactTest "flow-nexus"
            "store::tests::a_v1_leave_out_default_mixing_harnesses_refuses_the_upgrade_by_its_entry";
          flow-codex-frame-sixty-four-bit-length = context.exactTest "flow-nexus"
            "codex::tests::an_outbound_frame_past_sixty_five_kilobytes_takes_the_sixty_four_bit_length";
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
            "tests::reservation::a_claude_launch_whose_flow_id_cannot_be_claimed_is_refused_before_any_pane";
          flow-reserved-flow-id-reaches-the-harness = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_reserved_claude_launch_starts_as_its_session_with_its_flow_id";
          flow-reserved-flow-id-in-the-environment = context.exactTest "flow-nexus"
            "herdr::launch::tests::claude_environment_preparation_exports_the_reserved_flow_id";
          flow-launch-exports-its-nexus-socket = context.exactTest "flow-nexus"
            "herdr::launch::tests::claude_environment_preparation_exports_the_launching_nexus_socket";
          flow-launch-socket-from-the-start = context.exactTest "flow-nexus"
            "tests::launching_nexus::a_launch_receives_the_socket_from_each_typed_start";
          flow-hook-reports-to-its-launching-nexus = context.exactTest "flow"
            "the_hook_reports_to_the_socket_its_launching_nexus_exported";
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
          flow-caller-pane-ambiguous-session-refused = context.exactTest "flow-nexus"
            "tests::a_pane_id_whose_herdr_server_serves_two_sessions_names_no_pane";
          flow-launched-seat-identifies-and-wakes = context.exactTest "flow-nexus"
            "tests::waking::a_launched_seat_identifies_through_its_pane_and_its_order_wakes_an_asleep_seat";
          flow-send-server-identifies-codex-thread = context.exactTest "flow-nexus"
            "tests::waking::a_trusted_send_server_identifies_the_awake_flow_of_its_codex_thread";
          flow-client-default-socket = context.exactTest "flow"
            "the_client_reaches_the_default_ordinary_socket_under_the_runtime_directory";
          flow-meta-client-default-socket = context.exactTest "flow-meta"
            "the_meta_client_reaches_the_default_meta_socket_under_the_runtime_directory";
          flow-configuration-only-over-meta = context.exactTest "flow-nexus"
            "configure_only_commits_before_normal_start_uses_the_declared_store";
          flow-configure-family-is-its-last-declaration = context.exactTest "flow-nexus"
            "tests::configuring::each_family_is_its_last_declaration";
          flow-launch-takes-replaced-defaults = context.exactTest "flow-nexus"
            "tests::configuring::a_launch_after_a_replaced_declaration_takes_the_new_defaults";
          flow-claude-named-at-launch = context.exactTest "flow-nexus"
            "herdr::launch::tests::claude_is_named_at_launch_and_its_title_read_back";
          flow-workspace-titled-both-harnesses = context.exactTest "flow-nexus"
            "herdr::launch::tests::both_harnesses_label_the_workspace_with_the_title_and_never_the_agent_name";
          flow-workspace-titles-distinct-per-flow = context.exactTest "flow-nexus"
            "herdr::launch::tests::two_flows_of_one_seat_get_distinct_workspace_labels";
          flow-workspace-label-refusal-refuses-title = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_refused_workspace_label_refuses_the_title";
          flow-displayed-agent-refusal-refuses-label = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_displayed_agent_that_is_not_the_title_refuses_the_pane_label";
          flow-first-turn-sends-before-any-answer = context.exactTest "flow-nexus"
            "tests::waking::a_launched_seat_sends_an_order_from_its_first_tool_call_before_any_answer";
          flow-unconfirmed-launch-gives-back-its-address = context.exactTest "flow-nexus"
            "tests::waking::an_unconfirmed_launch_that_fails_or_ends_leaves_no_awake_registration";
          flow-stopped-unconfirmed-launch-frees-its-address = context.exactTest "flow-nexus"
            "tests::an_unconfirmed_launch_stopped_leaves_its_address_to_a_fresh_start";
          flow-stale-hold-given-back-at-start = context.exactTest "flow-nexus"
            "tests::a_stale_hold_behind_an_ended_flow_is_given_back_at_start";
          flow-in-flight-launch-refuses-its-address = context.exactTest "flow-nexus"
            "tests::a_launch_in_flight_before_binding_refuses_another_start_at_its_address";
          flow-contradicting-receipt-rejects-the-launch = context.exactTest "flow-nexus"
            "tests::a_receipt_that_contradicts_the_launch_rejects_it";
          flow-stop-waits-for-the-seat-to-rest = context.exactTest "flow-nexus"
            "tests::waking::a_seat_herdr_shows_working_at_its_stop_sleeps_once_herdr_shows_it_done";
          flow-stop-names-why-a-seat-is-kept = context.exactTest "flow-nexus"
            "tests::waking::a_stop_that_leaves_its_seat_names_why";
          flow-hook-closed-before-the-rest-wait = context.exactTest "flow-nexus"
            "tests::waking::the_hook_has_its_answer_and_its_connection_closed_before_the_rest_wait_ends";
          flow-codex-stop-before-confirmation-fabricates-nothing = context.exactTest "flow-nexus"
            "tests::waking::a_codex_stop_before_confirmation_does_not_fabricate_completion";
          flow-claude-end-turn-with-sidechain-drains = context.exactTest "flow-nexus"
            "tests::queue_drain::a_claude_main_end_turn_with_a_running_sidechain_drains_on_its_own_stop";
          flow-claude-two-requests-drain-on-receipts = context.exactTest "flow-nexus"
            "tests::queue_drain::two_queued_claude_requests_drain_in_order_once_each_on_their_receipts";
          flow-claude-notification-settles-nothing = context.exactTest "flow-nexus"
            "tests::queue_drain::a_task_notification_that_takes_the_turn_first_settles_nothing";
          flow-claude-crash-reconciled-at-start = context.exactTest "flow-nexus"
            "tests::queue_drain::a_hand_over_a_crash_left_unreceived_is_reconciled_at_start_without_a_second_typing";
          flow-claude-turn-end-before-confirmation = context.exactTest "flow-nexus"
            "tests::queue_drain::a_claude_turn_end_recorded_before_confirmation_is_honoured_at_it";
          flow-queue-resolved-delivered-leaves-the-queue = context.exactTest "flow-nexus"
            "tests::queue_drain::an_uncertain_hand_over_resolved_delivered_leaves_the_queue_with_its_record";
          flow-queue-resolved-absent-one-new-attempt = context.exactTest "flow-nexus"
            "tests::queue_drain::an_uncertain_hand_over_resolved_absent_is_followed_by_exactly_one_new_attempt";
          flow-queue-resolution-refused-at-the-nexus = context.exactTest "flow-nexus"
            "tests::queue_drain::a_resolution_is_refused_without_a_peer_for_an_unknown_or_non_uncertain_attempt";
          flow-queue-resolved-delivered-survives-restart = context.exactTest "flow-nexus"
            "store::queue_delivery::tests::resolved_delivered_leaves_the_queue_with_its_record_and_survives_restart";
          flow-queue-resolved-absent-survives-restart = context.exactTest "flow-nexus"
            "store::queue_delivery::tests::resolved_absent_keeps_the_request_and_authorises_exactly_one_new_attempt";
          flow-queue-resolution-store-refusals = context.exactTest "flow-nexus"
            "store::queue_delivery::tests::a_resolution_is_refused_for_what_is_not_the_oldest_uncertain_attempt";
          flow-codex-crash-found-in-history = context.exactTest "flow-nexus"
            "tests::send_tool::a_codex_turn_a_crash_left_unacknowledged_is_found_in_thread_history";
          flow-codex-absent-handed-over-once = context.exactTest "flow-nexus"
            "tests::send_tool::a_codex_turn_absent_from_an_idle_thread_is_handed_over_again_once";
          flow-codex-undecidable-never-resent = context.exactTest "flow-nexus"
            "tests::send_tool::an_undecidable_codex_turn_stays_uncertain_and_is_never_resent";
          flow-v1-stop-upgrades-unknown-identity = context.exactTest "flow-nexus"
            "store::tests::a_v1_stop_upgrades_to_a_stop_of_unknown_identity";
          flow-stop-sleeps-the-metaflow = context.exactTest "flow-nexus"
            "tests::waking::the_stop_of_a_confirmed_seat_leaves_its_metaflow_asleep_and_resumable";
          flow-retired-seat-wakes-fresh = context.exactTest "flow-nexus"
            "tests::waking::a_retired_seat_sleeps_its_metaflow_and_its_order_wakes_it_fresh";
          flow-ended-awake-woken-by-an-order = context.exactTest "flow-nexus"
            "tests::waking::a_metaflow_left_awake_as_an_ended_flow_is_woken_by_an_order";
          flow-ended-awake-rested-at-start = context.exactTest "flow-nexus"
            "tests::waking::a_nexus_start_puts_every_metaflow_left_awake_as_an_ended_flow_to_sleep";
          flow-claude-system-prompt-from-held-file = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_multiline_claude_system_prompt_is_read_whole_from_the_held_file";
          flow-control-character-refused-before-herdr = context.exactTest "flow-nexus"
            "herdr::launch::tests::an_argument_with_a_control_character_is_refused_before_herdr";
          flow-long-command-refused-before-herdr = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_typed_command_longer_than_one_terminal_line_is_refused_before_herdr";
          flow-herdr-refusal-kept = context.exactTest "flow-nexus"
            "herdr::launch::tests::herdrs_refusal_is_kept_as_herdr_wrote_it";
          flow-herdr-refusal-answered = context.exactTest "flow-nexus"
            "tests::reservation::a_claude_start_herdr_refuses_is_answered_with_herdrs_own_refusal";
          flow-opened-thread-accounted = context.exactTest "flow-nexus"
            "codex::tests::an_opened_thread_is_accounted_for_and_archived";
          flow-v3-outcomes-upgrade-to-refused-spawn = context.exactTest "flow-nexus"
            "store::tests::v3_launch_outcomes_upgrade_to_the_refused_spawn_shape";
          flow-registration-stage-on-wire = context.exactTest "flow-nexus"
            "herdr::launch::tests::every_registration_stage_is_carried_on_the_wire";
          flow-registration-refusal-reported = context.exactTest "flow-nexus"
            "tests::reservation::a_refused_registration_is_reported_with_its_stage";
          flow-native-step-refusal-answered = context.exactTest "flow-nexus"
            "performing::failure_tests::a_binding_failure_is_answered_with_its_stage_and_allowlisted_category";
          flow-receipt-refusal-staged = context.exactTest "flow-nexus"
            "tests::a_receipt_that_contradicts_the_launch_rejects_it";
          flow-v4-outcomes-upgrade-to-staged-refusal = context.exactTest "flow-nexus"
            "store::tests::v4_launch_outcomes_upgrade_to_the_staged_refusal_shape";
          flow-codex-recovery-binds-recorded-thread = context.exactTest "flow-nexus"
            "tests::reservation::a_codex_launch_recovered_after_its_start_binds_to_the_recorded_thread";
          flow-codex-recovery-refused-at-bind = context.exactTest "flow-nexus"
            "tests::reservation::a_codex_launch_recovered_without_its_thread_is_refused_at_bind_on_the_wire";
          flow-codex-wake-refused-while-tui-runs = context.exactTest "flow-nexus"
            "tests::waking::a_wake_is_refused_while_a_remote_resumed_codex_tui_runs_the_thread";
          flow-codex-pane-other-thread-not-held = context.exactTest "flow-nexus"
            "tests::waking::a_recorded_codex_pane_reporting_another_thread_does_not_hold_the_flow";
          flow-wake-pane-check-codex-aware = context.exactTest "flow-nexus"
            "tests::waking::the_wake_pane_check_binds_a_codex_flow_by_pane_agent_and_thread";
          flow-live-seat-check-typed = context.exactTest "flow-nexus"
            "tests::waking::the_live_seat_check_names_why_a_recorded_pane_is_not_the_flows_own";
          flow-live-codex-other-thread-not-adopted = context.exactTest "flow-nexus"
            "tests::waking::a_live_codex_flow_whose_pane_runs_another_thread_is_not_adopted";
          flow-bind-existing-verified-with-herdr = context.exactTest "flow-nexus"
            "tests::meta_bind_existing_verifies_the_pane_process_and_session_with_herdr";
          flow-codex-terminal-title-thread-only = context.exactTest "flow-nexus"
            "herdr::launch::tests::codex_launch_selects_one_exact_executable_and_remote_socket";
          flow-refreshed-codex-wakes-to-new-thread = context.exactTest "flow-nexus"
            "tests::waking::a_refreshed_codex_address_wakes_fresh_to_a_new_thread_and_binds_to_it";
          flow-refreshed-claude-wakes-to-new-session = context.exactTest "flow-nexus"
            "tests::waking::a_refreshed_claude_address_wakes_fresh_to_a_new_session_and_binds_to_it";
          flow-send-tool-threads-attributed = context.exactTest "flow-nexus"
            "tests::send_tool::two_threads_are_each_attributed_to_their_own_metaflow";
          flow-send-tool-unowned-thread-refused = context.exactTest "flow-nexus"
            "tests::send_tool::a_call_from_a_thread_flow_does_not_hold_is_refused_and_queues_nothing";
          flow-send-tool-stale-binding-refused = context.exactTest "flow-nexus"
            "tests::send_tool::a_replayed_call_of_a_retired_or_superseded_flow_is_refused";
          flow-send-tool-crash-replay-queued-once = context.exactTest "flow-nexus"
            "tests::send_tool::a_call_queued_before_a_crash_is_answered_from_its_record_when_replayed";
          flow-send-tool-key-reuse-refused = context.exactTest "flow-nexus"
            "tests::send_tool::a_call_key_recorded_with_other_content_is_refused";
          flow-send-tool-pending-call-after-restart = context.exactTest "flow-nexus"
            "tests::send_tool::a_pending_call_is_answered_after_start_and_reconnect";
          flow-send-tool-malformed-refused = context.exactTest "flow-nexus"
            "tests::send_tool::a_malformed_call_is_refused_and_recorded";
          flow-send-tool-ungated-app-server-refused = context.exactTest "flow-nexus"
            "codex::tests::an_app_server_that_does_not_gate_the_send_tool_opens_no_thread";
          flow-queued-request-rendered-with-sender = context.exactTest "flow-nexus"
            "tests::send_tool::a_queued_request_is_rendered_with_its_sender";
          flow-send-tool-wake-survives-failed-answer = context.exactTest "flow-nexus"
            "tests::send_tool::an_asleep_target_wakes_once_though_the_answer_fails_and_the_call_is_replayed";
          flow-send-tool-replay-wakes-asleep-target = context.exactTest "flow-nexus"
            "tests::send_tool::a_replayed_call_wakes_the_asleep_target_its_first_handling_did_not";
          flow-send-tool-primary-result-notice-wake-nothing = context.exactTest "flow-nexus"
            "tests::send_tool::a_send_to_an_asleep_primary_or_of_a_result_or_notice_wakes_nothing";
          flow-nexus-start-wakes-each-due-metaflow-once = context.exactTest "flow-nexus"
            "tests::waking::a_nexus_start_wakes_a_seat_whose_wake_did_not_finish";
          flow-v1-metaflow-queue-upgrades-sender-unknown = context.exactTest "flow-nexus"
            "store::tests::a_v1_metaflow_queue_upgrades_with_its_senders_unknown";
          flow-first-turn-send-served-before-idle = context.exactTest "flow-nexus"
            "tests::waking::a_first_turn_that_calls_send_before_idle_is_served_and_the_launch_completes";
          flow-refused-launch-keeps-first-turn-call = context.exactTest "flow-nexus"
            "tests::waking::a_launch_refused_after_its_first_turn_called_send_archives_its_thread_and_keeps_the_call";
          flow-interrupted-launch-reconciled-at-restart = context.exactTest "flow-nexus"
            "tests::waking::a_launch_interrupted_with_its_first_call_pending_is_refused_archived_and_settled_at_restart";
          flow-launch-address-held-elsewhere-sends-nothing = context.exactTest "flow-nexus"
            "tests::waking::a_launch_whose_address_another_flow_holds_sends_nothing";
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
