{
  description = "Typed durable Flow Nexus and clients.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
    # The Curriculum whose schema Flow pins (crates/flow-nexus/ethos/
    # curriculum.ethos, 478cbc5's byte for byte): the capsule runs its own
    # Curriculum Nexus from it.
    curriculum = {
      url = "github:LiGoldragon/Curriculum/478cbc51878921a0668278120aacca0031a72f92";
      flake = false;
    };
    # The public default remains the last unconfigured Flow artifact while
    # callers move explicitly to lib.<system>.mkFlowPackage.
    unconfigured-flow.url = "github:LiGoldragon/flow/d3ab87ee18b1dd30947d5f0089f5abb4bafa80d5";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
      curriculum,
      unconfigured-flow,
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
          # script, which holds the committed generated module fresh; the
          # Curriculum frames a test reads are raw rkyv.
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            name = "source";
            filter =
              path: type:
              (pkgs.lib.hasSuffix ".ethos" path)
              || (pkgs.lib.hasSuffix ".rkyv" path)
              || (craneLib.filterCargoSources path type);
          };
          commonArgs = {
            inherit src;
            pname = "flow-workspace";
            version = "0.53.2";
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
          exactGitTest =
            package: testName:
            (exactTest package testName).overrideAttrs (old: {
              nativeCheckInputs = (old.nativeCheckInputs or [ ]) ++ [ pkgs.git ];
            });
          # One named test across the workspace, run exactly once, for a
          # test that drives the workspace's own CLIs, which a test of one
          # package does not build.
          exactWorkspaceTest =
            testName:
            craneLib.mkCargoDerivation (
              commonArgs
              // {
                inherit cargoArtifacts;
                pnameSuffix = "-test";
                doCheck = true;
                buildPhaseCargoCommand = "";
                checkPhaseCargoCommand = ''
                  cargoWithProfile test --locked --workspace ${testName} -- --exact 2>&1 | tee exact-test.log
                  status=''${PIPESTATUS[0]}
                  if [ "$status" -ne 0 ]; then
                    exit "$status"
                  fi
                  ran=$(grep -cFx "test ${testName} ... ok" exact-test.log || true)
                  if [ "$ran" -ne 1 ]; then
                    echo "exact test ${testName} ran $ran times; it must run once" >&2
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
            exactGitTest
            exactWorkspaceTest
            lawSource
            ;
        };
    in
    {
      lib = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          mkFlowPackage = import ./nix/mk-flow-package.nix {
            inherit (nixpkgs) lib;
            inherit (context) pkgs craneLib commonArgs cargoArtifacts;
          };
        }
      );

      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          # Intentionally the last unconfigured artifact. A 0.43 package
          # is built only through lib.<system>.mkFlowPackage with an explicit
          # defaultConfiguration; production has supplied none yet.
          default = unconfigured-flow.packages.${system}.default;
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
          # The Curriculum Nexus and CLI of the Curriculum revision Flow
          # pins, as the capsule runs them.
          curriculum = context.craneLib.buildPackage {
            pname = "curriculum";
            version = "0.1.0";
            src = curriculum;
            strictDeps = true;
            cargoExtraArgs = "--locked --features datom --bins";
            doCheck = false;
          };
          # The capsule test: a Babashka script and the Configure datoms it
          # applies, kept side by side, with the Curriculum it runs. It is
          # given the flow package to run.
          flow-capsule = context.pkgs.runCommand "flow-capsule" { } ''
            mkdir -p "$out/libexec/flow-capsule" "$out/bin"
            ln -s ${self.packages.${system}.curriculum} "$out/libexec/flow-capsule/curriculum"
            {
              echo "#!${context.pkgs.babashka}/bin/bb"
              cat ${./capsule/flow-capsule.clj}
            } > "$out/libexec/flow-capsule/flow-capsule"
            chmod +x "$out/libexec/flow-capsule/flow-capsule"
            cp ${./capsule/configure.datom} "$out/libexec/flow-capsule/configure.datom"
            ln -s "$out/libexec/flow-capsule/flow-capsule" "$out/bin/flow-capsule"
          '';
        }
      );

      apps = forSystems (system: {
        flow-capsule = {
          type = "app";
          program = "${self.packages.${system}.flow-capsule}/bin/flow-capsule";
        };
      });

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
              # The approval pipeline's tests read a real git repository.
              nativeCheckInputs = [ context.pkgs.git ];
            }
          );
          fmt = context.craneLib.cargoFmt context.commonArgs;
          # The capsule runner's own logic that runs no capsule: where a run
          # keeps its witnesses and what it copies there before teardown.
          capsule-witnesses =
            context.pkgs.runCommand "flow-capsule-witnesses"
              { nativeBuildInputs = [ context.pkgs.babashka ]; }
              ''
                bb ${./capsule/flow-capsule-test.clj} ${./capsule/flow-capsule.clj} ${self.packages.${system}.default} > result.txt \
                  || { cat result.txt; exit 1; }
                cat result.txt
                cp result.txt "$out"
              '';
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
              cargoClippyExtraArgs = "--workspace --all-targets --features flow-ethos/datom -- -D warnings";
            }
          );
          flow-codex-thread-opened-lean-before-attach = context.exactTest "flow-nexus"
            "codex::tests::a_codex_thread_is_opened_lean_and_named_with_no_turn_before_attach";
          flow-codex-opened-name-readback-refused = context.exactTest "flow-nexus"
            "codex::tests::an_opened_thread_whose_name_reads_back_otherwise_is_archived";
          flow-codex-loaded-instructions-get-no-turn = context.exactTest "flow-nexus"
            "codex::tests::a_codex_thread_that_loaded_left_out_instructions_gets_no_turn";
          flow-codex-exclusions-reapplied-after-attach = context.exactTest "flow-nexus"
            "codex::tests::after_attach_the_exclusions_are_applied_again_and_read_back";
          flow-codex-attached-readback-refused = context.exactTest "flow-nexus"
            "codex::tests::an_attached_thread_whose_readback_differs_is_refused";
          flow-codex-first-turn-read-from-thread-start = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_codex_first_turn_is_read_from_the_thread_start";
          flow-codex-left-out-block-refused = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_codex_request_carrying_a_left_out_block_is_refused";
          flow-codex-tui-attaches-to-opened-thread = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_codex_tui_attaches_to_the_opened_thread_and_binds_only_to_it";
          flow-codex-remote-resumed-tui-binds-opened-thread = context.exactTest "flow-nexus"
            "codex::tests::a_remote_resumed_tui_binds_to_the_thread_the_app_server_opened";
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
          flow-start-is-one-message = context.exactTest "flow-nexus"
            "composition::tests::a_message_too_large_for_one_message_is_sent_in_parts_and_never_moves_to_the_system_prompt";
          flow-launch-carries-its-flow-id = context.exactTest "flow-nexus"
            "composition::tests::a_launch_carries_its_flow_id_first_in_its_launch_section";
          flow-codex-seat-told-its-flow-id = context.exactTest "flow-nexus"
            "codex::tests::after_attach_the_exclusions_are_applied_again_and_read_back";
          flow-earlier-store-refused-unchanged = context.exactTest "flow-nexus"
            "store::tests::an_earlier_flows_store_is_refused_and_left_byte_for_byte";
          flow-earlier-family-refused-not-upgraded = context.exactTest "flow-nexus"
            "store::tests::a_family_under_an_earlier_identity_is_refused_not_upgraded";
          flow-store-check-refuses-an-earlier-store = context.exactTest "flow-nexus"
            "store_check::tests::a_store_an_earlier_flow_wrote_is_refused";
          flow-task-failure-to-its-sender = context.exactTest "flow-nexus"
            "tests::reminding::a_tasks_last_reminder_sends_its_failure_to_the_tasks_own_sender";
          flow-self-task-failure-no-loop = context.exactTest "flow-nexus"
            "tests::reminding::a_task_a_seat_sent_itself_fails_to_it_once_with_no_loop";
          flow-refused-successor-told-to-no-flow = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::a_seat_whose_successor_cannot_compose_stays_and_is_handed_its_queue";
          flow-store-check-refuses-a-later-store = context.exactTest "flow-nexus"
            "store_check::tests::a_store_a_later_flow_wrote_is_refused";
          flow-codex-subagent-binds-its-seat = context.exactTest "flow-nexus"
            "tests::seat_binding::a_codex_subagent_binds_its_flows_seat_which_herdr_reports_no_session_for";
          flow-codex-ordinary-process-authority = context.exactWorkspaceTest
            "tests::seat_binding::ordinary_codex_cli_send_keep_and_drop_share_process_authority";
          flow-address-process-never-inherits-successor = context.exactTest "flow-nexus"
            "tests::seat_binding::a_registered_address_process_never_inherits_its_successor_flow";
          flow-failed-process-authority-never-owner = context.exactTest "flow-nexus"
            "tests::seat_binding::failed_process_authority_evidence_never_becomes_owner";
          flow-claude-seat-binds-on-an-empty-store = context.exactTest "flow-nexus"
            "tests::seat_binding::a_claude_seat_binds_on_an_empty_store_under_its_sessions_flow_id";
          flow-pane-without-agent-refuses-with-evidence = context.exactTest "flow-nexus"
            "tests::seat_binding::a_pane_with_no_agent_record_refuses_with_its_evidence";
          flow-store-check-opens-untouched = context.exactTest "flow-nexus"
            "store_check::tests::a_store_this_flow_opens_is_checked_open_and_left_untouched";
          flow-refresh-past-model-threshold = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::a_seat_resting_past_its_threshold_is_reaped_and_its_work_kept_for_the_next_flow";
          flow-deliver-past-threshold-refused = context.exactTest "flow-nexus"
            "tests::delivery::a_deliver_into_a_flow_past_its_threshold_is_refused";
          flow-codex-context-measured = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::a_codex_flow_s_context_is_its_thread_s_last_input";
          flow-one-message-through-herdr-socket = context.exactTest "flow-nexus"
            "herdr::launch::tests::stages_pane_native_claim_registration_and_one_ambiguous_prompt_write";
          flow-parts-received-in-order = context.exactTest "flow-nexus"
            "herdr::launch::tests::a_two_part_claude_message_is_received_part_by_part_in_order";
          flow-parts-advance-one-at-a-time = context.exactTest "flow-nexus"
            "store::tests::parts_advance_one_at_a_time_and_the_receipt_waits_for_the_last";
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
          flow-codex-frame-sixty-four-bit-length = context.exactTest "flow-nexus"
            "codex::tests::an_outbound_frame_past_sixty_five_kilobytes_takes_the_sixty_four_bit_length";
          flow-gone-pane-lists-exited = context.exactTest "flow-nexus"
            "tests::a_flow_whose_pane_left_herdr_is_listed_exited_and_keeps_its_row";
          flow-unreadable-herdr-changes-nothing = context.exactTest "flow-nexus"
            "tests::an_unreadable_herdr_leaves_a_listed_flow_as_it_stands";
          flow-live-pane-lists-active = context.exactTest "flow-nexus"
            "tests::a_pending_seat_whose_pane_herdr_shows_live_is_listed_active";
          flow-caller-server-known-by-session-socket = context.exactTest "flow-nexus"
            "tests::a_pane_id_names_the_session_its_server_serves_whatever_herdr_path_names";
          flow-ended-binding-forgotten = context.exactTest "flow-nexus"
            "tests::waking::an_address_held_only_through_an_ended_binding_is_forgotten";
          flow-ended-binding-with-work-rests = context.exactTest "flow-nexus"
            "tests::waking::an_ended_binding_holding_work_rests_asleep";
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
          flow-launch-configures-its-hook-socket = context.exactTest "flow-nexus"
            "herdr::launch::tests::every_claude_hook_command_carries_the_typed_nexus_socket";
          flow-launch-socket-from-the-start = context.exactTest "flow-nexus"
            "tests::launching_nexus::a_launch_receives_the_socket_from_each_typed_start";
          flow-two-explicit-nexuses-are-isolated = context.exactTest "flow-nexus"
            "two_explicit_starts_serve_disjoint_sockets_and_memory";
          flow-hook-reports-to-its-launching-nexus = context.exactTest "flow"
            "the_hook_reports_only_to_its_typed_socket_configuration";
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
          flow-claude-draft-refuses-hard-before-any-key = context.exactTest "flow-nexus"
            "tests::submission::a_draft_already_in_claudes_composer_refuses_a_hard_abrupt_before_any_key";
          flow-claude-cancelled-prompt-submitted-again = context.exactTest "flow-nexus"
            "tests::submission::a_prompt_claudes_interrupt_put_back_is_submitted_again_and_the_hard_abrupt_follows";
          flow-caller-pane-from-marks-or-ancestry = context.exactTest "flow-nexus"
            "tests::a_process_in_a_pane_is_found_by_its_own_marks_or_its_ancestors";
          flow-caller-pane-ambiguous-session-refused = context.exactTest "flow-nexus"
            "tests::a_pane_id_whose_herdr_server_serves_two_sessions_names_no_pane";
          flow-launched-seat-identifies-and-wakes = context.exactTest "flow-nexus"
            "tests::waking::a_launched_seat_identifies_through_its_pane_and_its_order_wakes_an_asleep_seat";
          flow-send-server-identifies-codex-thread = context.exactTest "flow-nexus"
            "tests::waking::a_trusted_send_server_identifies_the_awake_flow_of_its_codex_thread";
          flow-client-explicit-socket = context.exactTest "flow"
            "the_client_uses_only_the_explicit_socket";
          flow-meta-client-explicit-socket = context.exactTest "flow-meta"
            "the_client_uses_only_the_explicit_socket";
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
          flow-stop-waits-for-the-seat-to-rest = context.exactTest "flow-nexus"
            "tests::waking::a_seat_herdr_shows_working_at_its_stop_is_idle_once_herdr_shows_it_done";
          flow-stop-names-why-a-seat-is-kept = context.exactTest "flow-nexus"
            "tests::waking::a_stop_that_leaves_its_seat_names_why";
          flow-hook-closed-before-the-rest-wait = context.exactTest "flow-nexus"
            "tests::waking::the_hook_has_its_answer_and_its_connection_closed_before_the_rest_wait_ends";
          flow-claude-end-turn-with-sidechain-drains = context.exactTest "flow-nexus"
            "tests::queue_drain::a_claude_main_end_turn_with_a_running_sidechain_drains_on_its_own_stop";
          flow-claude-two-requests-drain-on-receipts = context.exactTest "flow-nexus"
            "tests::queue_drain::two_queued_claude_requests_go_in_one_payload_and_settle_on_its_receipt";
          flow-claude-notification-settles-nothing = context.exactTest "flow-nexus"
            "tests::queue_drain::a_task_notification_that_takes_the_turn_first_settles_nothing";
          flow-claude-crash-reconciled-at-start = context.exactTest "flow-nexus"
            "tests::queue_drain::a_hand_over_a_crash_left_unreceived_is_reconciled_at_start_without_a_second_typing";
          flow-claude-turn-end-before-confirmation = context.exactTest "flow-nexus"
            "tests::queue_drain::a_claude_turn_end_recorded_before_confirmation_is_honoured_at_it";
          flow-queue-resolution-refused-at-the-nexus = context.exactTest "flow-nexus"
            "tests::queue_drain::a_resolution_is_refused_without_a_peer_for_an_unknown_or_non_uncertain_attempt";
          flow-queue-resolved-absent-survives-restart = context.exactTest "flow-nexus"
            "store::queue_delivery::tests::resolved_absent_keeps_the_request_and_authorises_exactly_one_new_attempt";
          flow-queue-resolution-store-refusals = context.exactTest "flow-nexus"
            "store::queue_delivery::tests::a_resolution_is_refused_for_what_is_not_the_oldest_uncertain_attempt";
          flow-codex-crash-found-in-history = context.exactTest "flow-nexus"
            "tests::send_tool::a_codex_turn_a_crash_left_unacknowledged_is_found_in_thread_history";
          flow-codex-undecidable-never-resent = context.exactTest "flow-nexus"
            "tests::send_tool::an_undecidable_codex_turn_stays_uncertain_and_is_never_resent";
          flow-stop-sleeps-the-metaflow = context.exactTest "flow-nexus"
            "tests::waking::the_stop_of_a_confirmed_seat_leaves_its_metaflow_asleep_to_wake_fresh";
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
          flow-registration-stage-on-wire = context.exactTest "flow-nexus"
            "herdr::launch::tests::every_registration_stage_is_carried_on_the_wire";
          flow-registration-refusal-reported = context.exactTest "flow-nexus"
            "tests::reservation::a_refused_registration_is_reported_with_its_stage";
          flow-native-step-refusal-answered = context.exactTest "flow-nexus"
            "performing::failure_tests::a_binding_failure_is_answered_with_its_stage_and_allowlisted_category";
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
          flow-bind-existing-verified-with-herdr = context.exactTest "flow-nexus"
            "tests::meta_bind_existing_verifies_the_pane_process_and_session_with_herdr";
          flow-bind-existing-held-role-only = context.exactTest "flow-nexus"
            "tests::a_matching_binding_of_a_flow_already_held_asserts_only_its_role";
          flow-renamed-agent-current-name = context.exactTest "flow-nexus"
            "tests::a_renamed_agent_keeps_its_route_and_reports_its_current_name";
          flow-codex-terminal-title-thread-only = context.exactTest "flow-nexus"
            "herdr::launch::tests::codex_launch_selects_one_exact_executable_and_remote_socket";
          flow-sleeping-codex-wakes-to-new-thread = context.exactTest "flow-nexus"
            "tests::waking::a_sleeping_codex_address_wakes_fresh_to_a_new_thread_and_binds_to_it";
          flow-sleeping-claude-wakes-to-new-session = context.exactTest "flow-nexus"
            "tests::waking::a_sleeping_claude_address_wakes_fresh_to_a_new_session_and_binds_to_it";
          flow-idle-codex-keeps-its-pane = context.exactTest "flow-nexus"
            "tests::waking::an_idle_maintained_codex_flow_keeps_its_pane_and_stays_awake";
          flow-idle-claude-keeps-its-pane = context.exactTest "flow-nexus"
            "tests::waking::an_idle_maintained_claude_flow_keeps_its_pane_and_stays_awake";
          flow-idle-on-demand-claude-sleeps = context.exactTest "flow-nexus"
            "tests::waking::an_idle_on_demand_claude_flow_closes_its_harness_and_keeps_its_address";
          flow-maintained-refresh-successor-first = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::a_maintained_seat_refreshed_with_nothing_queued_starts_its_successor_first";
          flow-waking-refresh-successor-first = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::an_on_demand_seat_refreshed_with_a_waking_request_starts_its_successor_first";
          flow-maintained-exit-owes-successor = context.exactTest "flow-nexus"
            "tests::waking::a_maintained_metaflow_ended_by_an_exit_is_owed_a_successor";
          flow-terminal-sender-retracts-locks = context.exactTest "flow-nexus"
            "tests::waking::a_terminal_sender_retracts_its_locks";
          flow-observed-gone-releases-lock = context.exactTest "flow-nexus"
            "tests::waking::a_sender_observe_reports_gone_is_exited_and_its_lock_released";
          flow-topic-refused = context.exactTest "flow-nexus"
            "tests::start_admission::a_topic_that_is_not_camel_case_is_refused_at_start_and_configure";
          flow-start-waits-for-no-other-seat = context.exactTest "flow-nexus"
            "tests::start_admission::a_start_waits_for_no_other_models_seat";
          flow-declared-tool-set-passed-exactly = context.exactTest "flow-nexus"
            "lean::tests::a_declared_tool_set_is_passed_exactly_and_the_default_passes_none";
          flow-remembered-records-composed = context.exactTest "flow-nexus"
            "composition::tests::remembered_flows_raw_records_follow_the_user_modules_in_the_first_message";
          flow-nothing-starts-first = context.exactTest "flow-nexus"
            "tests::waking::a_start_starts_no_other_models_seat_first";
          flow-session-end-exits = context.exactTest "flow-nexus"
            "tests::waking::a_reported_session_end_exits_the_flow_and_owes_a_maintained_successor";
          flow-modules-placed-by-kind = context.exactTest "flow-nexus"
            "composition::tests::modules_are_placed_by_kind_spirit_and_intent_in_the_system_prompt";
          flow-idle-claude-request-into-its-pane = context.exactTest "flow-nexus"
            "tests::waking::a_request_to_an_idle_awake_claude_flow_is_delivered_into_its_pane";
          flow-idle-codex-request-into-its-pane = context.exactTest "flow-nexus"
            "tests::send_tool::a_request_to_an_idle_awake_codex_flow_is_delivered_into_its_pane";
          flow-gone-pane-asleep-wakes-fresh = context.exactTest "flow-nexus"
            "tests::waking::a_flow_whose_pane_is_gone_is_asleep_and_wakes_fresh";
          flow-order-wakes-primary-fresh = context.exactTest "flow-nexus"
            "tests::waking::an_order_wakes_an_asleep_primary_fresh";
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
          flow-nexus-start-wakes-each-due-metaflow-once = context.exactTest "flow-nexus"
            "tests::waking::a_nexus_start_wakes_a_seat_whose_wake_did_not_finish";
          flow-codex-long-first-turn-refuses-nothing = context.exactTest "flow-nexus"
            "tests::waking::a_codex_first_turn_starts_after_the_tui_attached_and_a_long_turn_refuses_nothing";
          flow-refused-codex-start-archives-idle-thread = context.exactTest "flow-nexus"
            "tests::waking::a_refused_codex_start_archives_its_idle_thread_and_starts_no_turn";
          flow-interrupted-launch-reconciled-at-restart = context.exactTest "flow-nexus"
            "tests::waking::a_launch_interrupted_with_its_first_call_pending_is_refused_archived_and_settled_at_restart";
          flow-launch-address-held-elsewhere-sends-nothing = context.exactTest "flow-nexus"
            "tests::waking::a_launch_whose_address_another_flow_holds_sends_nothing";
          # Restart, slice 1: a Claude flow, Awake and resting, asked from outside.
          flow-restart-awake-resting-claude-resumes-same-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_awake_resting_claude_resumes_same_session";
          flow-restart-arrival-queued-delivered-once = context.exactTest "flow-nexus"
            "tests::restarting::restart_arrival_queued_delivered_once";
          flow-restart-old-session-end-fenced = context.exactTest "flow-nexus"
            "tests::restarting::restart_old_session_end_fenced";
          flow-restart-new-seat-real-exit-ends = context.exactTest "flow-nexus"
            "tests::restarting::restart_new_seat_real_exit_ends";
          flow-restart-resumed-hooks-report-to-live-nexus = context.exactTest "flow-nexus"
            "tests::restarting::restart_resumed_hooks_report_to_live_nexus";
          flow-restart-close-refused-flow-unchanged = context.exactTest "flow-nexus"
            "tests::restarting::restart_close_refused_flow_unchanged";
          flow-restart-resume-refused-custody-kept = context.exactTest "flow-nexus"
            "tests::restarting::restart_resume_refused_custody_kept";
          flow-restart-unknown-flow = context.exactTest "flow-nexus"
            "tests::restarting::restart_unknown_flow";
          flow-restart-no-held-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_no_held_session";
          flow-restart-transcript-absent = context.exactTest "flow-nexus"
            "tests::restarting::restart_transcript_absent";
          flow-restart-busy = context.exactTest "flow-nexus"
            "tests::restarting::restart_busy";
          flow-restart-bind-mismatch = context.exactTest "flow-nexus"
            "tests::restarting::restart_bind_mismatch";
          flow-restart-inspect-shows-record = context.exactTest "flow-nexus"
            "tests::restarting::restart_inspect_shows_record";
          flow-restart-nexus-death-each-phase = context.exactTest "flow-nexus"
            "tests::restarting::restart_nexus_death_each_phase";
          flow-restart-crash-around-bind-commit-delivers-once = context.exactTest "flow-nexus"
            "tests::restarting::restart_crash_around_bind_commit_delivers_once";
          flow-restart-interrupted-stop-needs-quiesced-spawn = context.exactTest "flow-nexus"
            "tests::restarting::restart_interrupted_stop_needs_quiesced_spawn";
          # 0.53.1: the resume is composed and validated before the old harness closes.
          flow-restart-composition-refused-old-harness-untouched = context.exactTest "flow-nexus"
            "tests::restarting::restart_composition_refused_old_harness_untouched";
          flow-restart-root-refused-old-harness-untouched = context.exactTest "flow-nexus"
            "tests::restarting::restart_root_refused_old_harness_untouched";
          flow-restart-harness-flags-refused-old-harness-untouched = context.exactTest "flow-nexus"
            "tests::restarting::restart_harness_flags_refused_old_harness_untouched";
          flow-restart-command-line-refused-old-harness-untouched = context.exactTest "flow-nexus"
            "tests::restarting::restart_command_line_refused_old_harness_untouched";
          flow-restart-precomposed-success-closes-once-resumes-same-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_precomposed_success_closes_once_resumes_same_session";
          flow-restart-retry-after-composition-refused-restarts = context.exactTest "flow-nexus"
            "tests::restarting::restart_retry_after_composition_refused_restarts";
          flow-restart-crash-after-closing-reconciles = context.exactTest "flow-nexus"
            "tests::restarting::restart_crash_after_closing_reconciles";
          flow-restart-crash-after-closing-composition-refused-old-harness-untouched = context.exactTest "flow-nexus"
            "tests::restarting::restart_crash_after_closing_composition_refused_old_harness_untouched";
          flow-restart-failed-not-composed-by-0530-retry-resumes-same-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_failed_not_composed_by_0530_retry_resumes_same_session";
          # 0.53.1: a resume that fails after the close reopens the same
          # session by itself, once, once nothing holds the session.
          flow-restart-spawn-refused-once-reopens-same-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_spawn_refused_once_reopens_same_session";
          flow-restart-bind-mismatch-once-reopens-same-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_bind_mismatch_once_reopens_same_session";
          flow-restart-failure-every-time-spends-bound-custody-kept = context.exactTest "flow-nexus"
            "tests::restarting::restart_failure_every_time_spends_bound_custody_kept";
          flow-restart-start-in-flight-not-respawned = context.exactTest "flow-nexus"
            "tests::restarting::restart_start_in_flight_not_respawned";
          flow-restart-nexus-crash-after-post-close-failure-recovers = context.exactTest "flow-nexus"
            "tests::restarting::restart_nexus_crash_after_post_close_failure_recovers";
          flow-restart-owner-stop-during-bound = context.exactTest "flow-nexus"
            "tests::restarting::restart_owner_stop_during_bound";
          flow-restart-failed-before-close-taken-up-once-old-harness-gone = context.exactTest "flow-nexus"
            "tests::restarting::restart_failed_before_close_taken_up_once_old_harness_gone";
          # 0.53.1: a reopen, by itself or the owner's, needs the failed
          # seat's harness recorded and proven exited by its birth.
          flow-restart-live-orphan-not-reopened-in-turn = context.exactTest "flow-nexus"
            "tests::restarting::restart_live_orphan_not_reopened_in_turn";
          flow-restart-live-orphan-not-reopened-at-start = context.exactTest "flow-nexus"
            "tests::restarting::restart_live_orphan_not_reopened_at_start";
          flow-restart-missing-witness-not-reopened = context.exactTest "flow-nexus"
            "tests::restarting::restart_missing_witness_not_reopened_retry_refused";
          flow-restart-recorded-harness-exited-reopens-same-session = context.exactTest "flow-nexus"
            "tests::restarting::restart_recorded_harness_exited_reopens_same_session";
          flow-restart-pid-reused-exited-by-birth = context.exactTest "flow-nexus"
            "tests::restarting::restart_pid_reused_exited_by_birth_only";
          flow-restart-startup-recovery-uses-recorded-identity = context.exactTest "flow-nexus"
            "tests::restarting::restart_startup_recovery_uses_recorded_identity";
          # 0.53.1: an exit is proven only by a pid not found, another birth
          # at the pid, or a Z or X state; anything else is unproven. A
          # first restart whose old pane is gone needs its recorded harness
          # so proven, before any write.
          flow-restart-exit-proof-not-found = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_not_found_proves_exit";
          flow-restart-exit-proof-other-birth = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_other_birth_proves_exit";
          flow-restart-exit-proof-zombie-and-dead = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_zombie_and_dead_prove_exit";
          flow-restart-exit-proof-permission-denied-unproven = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_permission_denied_unproven";
          flow-restart-exit-proof-io-error-unproven = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_io_error_unproven";
          flow-restart-exit-proof-malformed-stat-unproven = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_malformed_stat_unproven";
          flow-restart-exit-proof-unparseable-start-time-unproven = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_unparseable_start_time_unproven";
          flow-restart-exit-proof-invalid-pid-unproven = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_invalid_pid_unproven";
          flow-restart-exit-proof-unparsed-birth-unproven = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_unparsed_birth_unproven";
          flow-restart-exit-proof-live-states-running = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_live_states_same_birth_running";
          flow-restart-exit-proof-proc-reads-the-kernel = context.exactTest "flow-nexus"
            "tests::restarting::restart_exit_proof_proc_reads_the_kernel";
          flow-restart-first-old-pane-gone-harness-alive-refused = context.exactTest "flow-nexus"
            "tests::restarting::restart_first_old_pane_gone_harness_alive_refused";
          flow-restart-first-old-pane-gone-unrecorded-refused-unwitnessed = context.exactTest "flow-nexus"
            "tests::restarting::restart_first_old_pane_gone_unrecorded_refused_unwitnessed";
          flow-restart-first-old-pane-gone-harness-exited-proceeds = context.exactTest "flow-nexus"
            "tests::restarting::restart_first_old_pane_gone_harness_exited_proceeds";
          # The cutover with both Nexuses live: the old side cannot take or end
          # the custody a new Nexus Adopted and Restarted.
          flow-restart-cutover-two-nexus = context.exactTest "flow-nexus"
            "tests::restarting::restart_cutover_two_nexus";
          # 0.53.1: a protected Address answers only its own seat, the meta
          # surface and, for Stop and Retire, the owner; it is never
          # refreshed for its threshold.
          flow-restart-protected-from-outside-refused-no-effect = context.exactTest "flow-nexus"
            "tests::restarting::restart_protected_from_outside_refused_no_effect";
          flow-restart-protected-self-accepted = context.exactTest "flow-nexus"
            "tests::restarting::restart_protected_self_accepted";
          flow-restart-protected-from-meta-accepted = context.exactTest "flow-nexus"
            "tests::restarting::restart_protected_from_meta_accepted";
          flow-protected-stop-and-retire-from-outside-refused = context.exactTest "flow-nexus"
            "tests::restarting::stop_and_retire_protected_from_outside_refused";
          flow-protected-past-threshold-kept = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::a_protected_seat_past_its_threshold_is_kept_and_handed_its_work";
          flow-unprotected-fable-past-threshold-refreshed = context.exactTest "flow-nexus"
            "tests::refresh_and_compaction::an_unprotected_fable_seat_past_its_threshold_is_refreshed";
          flow-protected-addresses-persist-across-reopen = context.exactTest "flow-nexus"
            "tests::configuring::protected_addresses_persist_across_reopen";
          flow-store-0530-opens-unprotected = context.exactTest "flow-nexus"
            "store::tests::a_store_0530_wrote_opens_with_no_address_protected";
          # 0.53.1: protos 0.33, datom-codec 0.33, signal 10.3: a Send holding
          # nested and escaped guillemets round-trips through the real CLIs
          # and the Nexus.
          flow-send-guillemets-round-trip = context.exactWorkspaceTest
            "tests::send_admission::a_send_with_nested_and_escaped_guillemets_round_trips_through_the_cli_and_the_nexus";
          # 0.53.1: Flow reads Curriculum 478c's schema; real 478c frames, and
          # the 0.53.0 NotComposed incident against a socket Curriculum.
          flow-curriculum-478c-rejected-frame = context.exactTest "flow-nexus"
            "tests::curriculum::a_real_478c_rejected_frame_is_a_composer_rejection_carrying_its_text";
          flow-curriculum-478c-startup-composed-frame = context.exactTest "flow-nexus"
            "tests::curriculum::a_real_478c_startup_composed_frame_reads_and_verifies";
          # 0.53.1: a successor's first prompt ends with its predecessor's handover.
          flow-handover-waking-successor-last-whole = context.exactTest "flow-nexus"
            "tests::handover::a_waking_successor_is_given_its_predecessors_handover_last_whole";
          flow-handover-refresh-successor-last-whole = context.exactTest "flow-nexus"
            "tests::handover::a_refresh_successor_is_given_its_predecessors_handover_last_whole";
          flow-handover-absent-first-prompt-unchanged = context.exactTest "flow-nexus"
            "tests::handover::a_successor_whose_predecessor_left_no_file_has_its_first_prompt_unchanged";
          flow-handover-unreadable-waking-refused-asleep = context.exactTest "flow-nexus"
            "tests::handover::a_waking_whose_handover_cannot_be_read_is_refused_asleep_with_its_queue";
          flow-handover-unreadable-refresh-refused-predecessor-kept = context.exactTest "flow-nexus"
            "tests::handover::a_refresh_whose_handover_cannot_be_read_keeps_its_predecessor";
          flow-handover-invalid-predecessor-refused = context.exactTest "flow-nexus"
            "tests::handover::a_waking_after_a_predecessor_naming_no_flow_is_refused_asleep_with_its_queue";
          flow-handover-root-unresolved-refused = context.exactTest "flow-nexus"
            "tests::handover::a_launch_whose_root_cannot_be_resolved_is_refused_naming_its_predecessor";
          flow-handover-on-demand-seat-kept = context.exactTest "flow-nexus"
            "tests::handover::an_on_demand_seat_whose_successor_is_refused_its_handover_is_not_closed";
          flow-handover-last-bytes-either-harness = context.exactTest "flow-nexus"
            "tests::handover::the_handover_is_the_last_bytes_of_the_first_prompt_on_either_harness";
          flow-handover-restart-none = context.exactTest "flow-nexus"
            "tests::handover::a_launch_naming_no_predecessor_is_given_no_handover";
          flow-handover-large-last-whole-part = context.exactTest "flow-nexus"
            "tests::handover::a_handover_larger_than_the_rest_of_the_message_is_the_last_whole_part";
          flow-handover-simplified = context.exactTest "flow-nexus"
            "tests::handover::a_simplified_flow_is_given_the_handover_after_its_first_message_only";
          flow-handover-file-source = context.exactTest "flow-nexus"
            "tests::handover::the_file_source_reads_a_predecessors_file_under_the_launch_root_only";
          flow-retirement-observation-fifo-overflow = context.exactTest "flow-nexus"
            "handover_observation::tests::overflow_is_an_out_of_band_terminal_after_the_complete_fifo";
          flow-retirement-observation-disconnect = context.exactTest "flow-nexus"
            "handover_observation::tests::disconnect_wakes_an_idle_subscription_as_incomplete";
          flow-retirement-attach-during-publication = context.exactTest "flow-nexus"
            "tests::handover_archive::attaching_during_publication_sees_closed_then_archive_then_removed";
          flow-retirement-attach-close-no-gap = context.exactTest "flow-nexus"
            "tests::handover_memory::concurrent_attach_and_close_places_the_generation_exactly_once_in_snapshot_or_fifo";
          flow-retirement-two-closes-fifo = context.exactTest "flow-nexus"
            "tests::handover_memory::two_closes_are_observed_in_their_durable_mutation_order";
          flow-retirement-delivery-outside-gate = context.exactTest "flow-nexus"
            "tests::handover_memory::queued_delivery_completes_while_the_handover_mutation_gate_is_held";
          flow-retirement-initial-write-detaches = context.exactTest "flow-nexus"
            "tests::handover_archive::initial_snapshot_write_failure_detaches_the_ephemeral_subscription";
          flow-retirement-archive-identity = context.exactGitTest "flow-nexus"
            "tests::handover_archive::archives_and_is_idempotent";
          flow-retirement-archive-byte-identity = context.exactGitTest "flow-nexus"
            "handover_archive::tests::invalid_utf8_git_evidence_is_rejected_instead_of_aliasing_replacement_text";
          flow-retirement-preserves-current-and-sibling = context.exactTest "flow-nexus"
            "tests::handover_memory::exact_retirement_identity_preserves_new_live_and_closed_rows";
          flow-retirement-identity-mismatch-incomplete = context.exactTest "flow-nexus"
            "tests::handover_memory::changed_published_snapshot_is_retained";
          flow-retirement-already-absent-incomplete = context.exactTest "flow-nexus"
            "tests::handover_memory::already_absent_is_explicitly_incomplete_and_never_emits_removed";
          flow-retirement-memory-failure-incomplete = context.exactTest "flow-nexus"
            "tests::handover_memory::verified_archive_then_memory_failure_reports_archive_and_incomplete_without_removed";
          flow-retirement-scan-read-failure-incomplete = context.exactTest "flow-nexus"
            "tests::handover_archive::retirement_scan_read_failure_is_explicitly_incomplete";
          flow-retirement-completion-read-failure-incomplete = context.exactTest "flow-nexus"
            "tests::handover_memory::retirement_completion_read_failure_is_explicitly_incomplete_and_retains_the_generation";
          flow-retirement-cli-incomplete-eof = context.exactTest "flow"
            "tests::retirement_eof_and_truncated_frames_are_incomplete";
          flow-retirement-cli-terminal-failure = context.exactTest "flow"
            "tests::a_terminal_retirement_failure_is_printed_and_fails_the_call";
          flow-retirement-launch-eof-unchanged = context.exactTest "flow"
            "tests::launch_observation_keeps_its_existing_clean_eof_semantics";
          flow-retirement-immutable-contract-pins = context.pkgs.runCommand
            "flow-retirement-immutable-contract-pins"
            { nativeBuildInputs = [ context.pkgs.gnugrep ]; }
            ''
              grep -F 'rev = "e050c8fbf334d859391cb64f6a47663a2c689c11"' ${./Cargo.toml}
              grep -F 'rev = "91e1f45adced679a006638f1cec1b6456e5a4dc0"' ${./Cargo.toml}
              grep -F 'signal-flow?rev=e050c8fbf334d859391cb64f6a47663a2c689c11#e050c8fbf334d859391cb64f6a47663a2c689c11' ${./Cargo.lock}
              grep -F 'meta-signal-flow?rev=91e1f45adced679a006638f1cec1b6456e5a4dc0#91e1f45adced679a006638f1cec1b6456e5a4dc0' ${./Cargo.lock}
              touch "$out"
            '';
          flow-handover-permission-denied-unreadable = context.exactTest "flow-nexus"
            "tests::handover::a_file_the_nexus_may_not_read_is_unreadable";
          flow-restart-not-composed-refused-then-recovered = context.exactTest "flow-nexus"
            "tests::restarting::restart_not_composed_refused_then_recovered";
          flow-restart-not-composed-owner-stop = context.exactTest "flow-nexus"
            "tests::restarting::restart_not_composed_owner_stop";
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
