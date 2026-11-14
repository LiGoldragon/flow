{
  description = "Typed durable Flow Nexus and clients.";

  inputs = {
    # The build base: nixpkgs, the Rust toolchain and the per-crate
    # machinery, shared with every repository on it, so a crate at one
    # version and feature set has one store path across them.
    base.url = "github:LiGoldragon/build-base";
    # The Curriculum whose schema Flow pins (crates/flow-nexus/ethos/
    # curriculum.ethos, 478cbc5's but for RolesDocument's bare Roles variant,
    # which ethos-zero 22 requires and which keeps every form): the capsule runs its own
    # Curriculum Nexus from it.
    curriculum = {
      url = "github:LiGoldragon/Curriculum";
      flake = false;
    };
    # The public default remains the last unconfigured Flow artifact while
    # callers move explicitly to lib.<system>.mkFlowPackage.
    unconfigured-flow.url = "github:LiGoldragon/flow/d3ab87ee18b1dd30947d5f0089f5abb4bafa80d5";
    # The accepted 0.53.2 Flow, built by its own flake, whose Memory the
    # request-id migration check evolves. The lock holds its commit, f7d2336,
    # and the flake refuses any other (predecessorRevision below).
    migration-predecessor.url = "github:LiGoldragon/flow";
  };

  # Every crate, third-party or member, is one buildRustCrate derivation.
  # Cargo.nix, and nix/curriculum/Cargo.nix for the pinned Curriculum, are
  # generated from their Cargo.lock by the build base's crate2nix and
  # committed, so evaluation imports no derivation. After editing a
  # Cargo.toml or Cargo.lock:
  #   nix run github:LiGoldragon/build-base#crate2nix -- generate
  outputs =
    {
      self,
      base,
      curriculum,
      unconfigured-flow,
      migration-predecessor,
    }:
    let
      lib = base.inputs.nixpkgs.lib;
      version = "0.55.0";
      forSystems = function: lib.genAttrs base.lib.systems function;
      # The revision the process checks bind their binaries to, given to the
      # test at run time: the committed revision, or the dirty one a working
      # tree has. No build reads it, so a commit rebuilds only what it changed.
      sourceRevision = self.rev or self.dirtyRev or "unpublished";
      # The accepted 0.53.2 revision the migration check names; a lock that
      # moved migration-predecessor elsewhere fails evaluation.
      predecessorRevision =
        assert lib.assertMsg (migration-predecessor.rev == "f7d2336a7380206728cbd5aef2ebb3656d0cfd87")
          "migration-predecessor must stay locked at the accepted 0.53.2, f7d2336a7380206728cbd5aef2ebb3656d0cfd87";
        migration-predecessor.rev;
      mkContext =
        system:
        let
          context = base.lib.contexts.${system};
          inherit (context) pkgs toolchain;
          # The base's workspace: each member's build and test build, and
          # checks.default, every check below with the base's cargo-nix. Each
          # member with tests is checked in its own environment, and the
          # frozen contract crates have no check.
          workspace = base.lib.workspace.${system} {
            cargoNix = ./Cargo.nix;
            memberChecks = _: memberChecks // lib.genAttrs untested (_: null);
            extraChecks =
              { doc, ... }:
              extraChecks
              // {
                # The base's rustdoc over each library, warnings denied; clippy
                # runs inside each member's test build.
                doc = pkgs.linkFarm "flow-doc" (
                  lib.mapAttrs (
                    name: features:
                    doc name features [
                      "-D"
                      "warnings"
                    ]
                  ) lintFeatures
                );
              };
          };
          members = removeAttrs workspace.packages [ "default" ];
          # The frozen pre-request-id contracts the memory upgrade reads.
          # They carry no tests; flow-nexus's tests exercise them.
          untested = [
            "flow-ethos-before-request-ids"
            "signal-flow-before-request-ids"
            "meta-signal-flow-before-request-ids"
          ];
          # flow-ethos is tested in its datom form, the one its dependents
          # build and the one its tests are written for, over the datom
          # dependencies flow builds it with.
          flowEthosWithDatom = lib.findFirst (
            dependency: dependency.crateName == "flow-ethos"
          ) (throw "flow builds no flow-ethos") members.flow.dependencies;
          testBuilds = workspace.tests // {
            flow-ethos = workspace.tests.flow-ethos.override {
              # The feature set flow already builds flow-ethos with, so the
              # test links that library rather than building another.
              features = flowEthosWithDatom.crateFeatures;
              inherit (flowEthosWithDatom) dependencies;
            };
          };
          # The binaries of builds copied into one bin directory: the daemon,
          # flow-nexus-daemon, finds flow-nexus-hook beside its own resolved
          # executable.
          binaries =
            name: builds: attributes:
            pkgs.runCommand name attributes ''
              mkdir -p "$out/bin"
              ${lib.concatMapStrings (build: ''
                if [ -d ${build}/bin ]; then cp -L ${build}/bin/* "$out/bin/"; fi
              '') builds}
            '';
          # Runs every test binary of one member's test build from a copy of
          # its source, the directory cargo runs tests in, and compiles
          # nothing. A failing test fails the check, and so does a test build
          # that ran no test.
          crateCheck =
            name: tests: environment:
            pkgs.runCommand "check-${name}"
              {
                nativeBuildInputs = environment.inputs or [ ];
                passthru = { inherit tests; };
              }
              ''
                set -o pipefail
                cp -r ${tests.src} "$NIX_BUILD_TOP/source"
                chmod -R u+w "$NIX_BUILD_TOP/source"
                # The test binaries run from one directory, as cargo leaves
                # them in target/<profile>/, beside the CLIs a test drives.
                binaries="$NIX_BUILD_TOP/binaries"
                mkdir "$binaries"
                ${lib.concatMapStrings (path: ''
                  ln -s ${path} "$binaries/"
                '') (environment.beside or [ ])}
                cd "$NIX_BUILD_TOP/source"
                passed=0
                ignored=0
                for test in ${tests}/tests/*; do
                  cp "$test" "$binaries/"
                  "$binaries/$(basename "$test")" 2>&1 | tee "$NIX_BUILD_TOP/test.log"
                  result=$(grep -E '^test result: ok\.' "$NIX_BUILD_TOP/test.log" | tail -n 1)
                  passed=$((passed + $(echo "$result" | sed -E 's/.* ([0-9]+) passed.*/\1/')))
                  ignored=$((ignored + $(echo "$result" | sed -E 's/.* ([0-9]+) ignored.*/\1/')))
                done
                if [ "$passed" -eq 0 ]; then
                  echo "${name}: no test ran" >&2
                  exit 1
                fi
                echo "${name}: $passed passed, $ignored ignored" | tee "$out"
              '';
          checkEnvironments = {
            # The approval pipeline's and the handover archive's tests read a
            # real git repository, one holds a pipe in a session of its own
            # (setsid), and two drive the flow-nexus and flow-nexus-meta CLIs.
            flow-nexus = {
              inputs = [
                pkgs.git
                pkgs.util-linux
              ];
              beside = [
                "${members.flow}/bin/flow-nexus"
                "${members.flow-meta}/bin/flow-nexus-meta"
              ];
            };
          };
          memberChecks = lib.mapAttrs (
            name: tests: crateCheck name tests (checkEnvironments.${name} or { })
          ) (removeAttrs testBuilds untested);
          # The one test binary of a test build that lists the named test,
          # ignored ones included; the shell variable it is bound to.
          findTest = variable: tests: testName: ''
            ${variable}=
            for candidate in ${tests}/tests/*; do
              if "$candidate" --list --ignored 2>/dev/null | grep -qFx "${testName}: test"; then
                if [ -n "''$${variable}" ]; then
                  echo "${testName} is listed by more than one test binary" >&2
                  exit 1
                fi
                ${variable}=$candidate
              fi
            done
            if [ -z "''$${variable}" ]; then
              echo "no test binary lists ${testName}" >&2
              exit 1
            fi
          '';
          # One ignored test of the flow-nexus test build, run exactly once
          # with the given environment from a copy of the source.
          processCheck =
            name: testName: environment:
            pkgs.runCommand "flow-${name}"
              {
                nativeBuildInputs = [
                  pkgs.git
                  pkgs.util-linux
                ];
              }
              ''
                set -o pipefail
                cp -r ${testBuilds.flow-nexus.src} "$NIX_BUILD_TOP/source"
                chmod -R u+w "$NIX_BUILD_TOP/source"
                cd "$NIX_BUILD_TOP/source"
                ${findTest "binary" testBuilds.flow-nexus testName}
                ${environment}
                "$binary" --exact ${testName} --ignored --nocapture 2>&1 | tee "$NIX_BUILD_TOP/${name}.log"
                ran=$(grep -cF "test ${testName} ... ok" "$NIX_BUILD_TOP/${name}.log" || true)
                if [ "$ran" -ne 1 ]; then
                  echo "${testName} ran $ran times; it must run once" >&2
                  exit 1
                fi
                cp "$NIX_BUILD_TOP/${name}.log" "$out"
              '';
          # The members that have binaries.
          commands = {
            inherit (members) flow flow-meta flow-nexus;
          };
          workspaceBinaries = binaries "flow-workspace-${version}" (lib.attrValues commands) { };
          # The feature sets each member is linted and documented under: every
          # member as its dependents build it, flow-ethos in its datom form.
          lintFeatures = lib.genAttrs (lib.attrNames workspace.tests) (_: [ "default" ]) // {
            flow-ethos = [
              "default"
              "datom"
            ];
          };
          # The Rust text alone, for the checks that read it and compile
          # nothing.
          rustSource = lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (type == "directory")
              || (lib.hasSuffix ".rs" path)
              || (baseNameOf path == "Cargo.toml")
              || (baseNameOf path == "Cargo.lock");
          };
          # The two trait laws are read off the Rust text, so their check
          # needs every `.rs` file plus the shell the check itself is written
          # in.
          lawSource = lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (type == "directory")
              || (type == "regular" && lib.hasSuffix ".rs" path)
              || (type == "regular" && lib.hasSuffix ".sh" path);
          };
          # The accepted 0.53.2 Flow, built by its own flake, whose Memory
          # the request-id migration evolves.
          migrationRoot = "/tmp/flow-request-id-migration-intermediate";
          migrationPredecessorPackage =
            (migration-predecessor.lib.${system}.mkFlowPackage {
              defaultConfiguration = {
                sourceRoot = "${migrationRoot}/source";
                runtimeDirectory = "${migrationRoot}/runtime";
                stableCodex = {
                  clientPath = "${migrationRoot}/bin/codex-stable";
                  home = "${migrationRoot}/codex-stable";
                  controlSocketPath = "${migrationRoot}/runtime/stable.sock";
                  roster = [ "migration-stable" ];
                };
                nextCodex = {
                  clientPath = "${migrationRoot}/bin/codex-next";
                  home = "${migrationRoot}/codex-next";
                  controlSocketPath = "${migrationRoot}/runtime/next.sock";
                  roster = [ "migration-next" ];
                };
                harnessProfiles = [
                  {
                    harnessKind = "Codex";
                    commandSigils = [ "/" ];
                    interruptKeys = [ "esc" ];
                    submitKeys = [ ];
                  }
                ];
                metaAspects = [ "Mind" ];
                presentationPrompt = null;
              };
            }).overrideAttrs
              (old: {
                nativeCheckInputs = (old.nativeCheckInputs or [ ]) ++ [ pkgs.util-linux ];
              });
          migrationPredecessorFixture = migrationPredecessorPackage.overrideAttrs (old: {
            pname = "flow-configured-request-id-predecessor-fixture";
            nativeCheckInputs = (old.nativeCheckInputs or [ ]) ++ [ pkgs.inotify-tools ];
            postPatch = (old.postPatch or "") + ''
              cp ${./crates/flow-nexus/src/store/request_id_predecessor_fixture.rs} \
                crates/flow-nexus/src/store/request_id_predecessor_fixture.rs
              cat >> crates/flow-nexus/src/store.rs <<'RUST'

              #[cfg(test)]
              #[path = "store/request_id_predecessor_fixture.rs"]
              mod request_id_predecessor_fixture;
              RUST
            '';
            checkPhase = ''
              runHook preCheck
              export FLOW_PREDECESSOR_FIXTURE_OUT="$out/share/request-id-predecessor"
              fixture_home="$TMPDIR/predecessor-home"
              fixture_runtime="$TMPDIR/predecessor-runtime"
              mkdir -m 700 -p "$fixture_home" "$fixture_runtime" "$FLOW_PREDECESSOR_FIXTURE_OUT"
              # The predecessor's socket is awaited as the inotify event of its
              # name, watched before the predecessor starts; each read is
              # bounded, so a predecessor that never binds fails the check.
              coproc runtime_events {
                exec inotifywait -m -e create -e moved_to --format '%f' "$fixture_runtime" 2>&1
              }
              while IFS= read -r -t 60 line <&"''${runtime_events[0]}"; do
                [ "$line" = "Watches established." ] && break
              done
              HOME="$fixture_home" XDG_RUNTIME_DIR="$fixture_runtime" \
                target/release/flow-nexus \
                "{ «$fixture_runtime/flow.sock» «$fixture_runtime/flow-meta.sock» «$FLOW_PREDECESSOR_FIXTURE_OUT/flow.sema» None }" &
              predecessor_pid=$!
              while IFS= read -r -t 60 name <&"''${runtime_events[0]}"; do
                [ "$name" = flow.sock ] && break
              done
              kill "$runtime_events_PID"
              if ! test -S "$fixture_runtime/flow.sock"; then
                echo "the exact predecessor never opened its socket" >&2
                kill -TERM "$predecessor_pid" 2>/dev/null || true
                wait "$predecessor_pid" || true
                exit 1
              fi
              kill -TERM "$predecessor_pid"
              wait "$predecessor_pid"
              cargoWithProfile test -p flow-nexus --lib \
                store::request_id_predecessor_fixture::writes_nonempty_request_id_predecessor_memory \
                -- --exact --ignored
              test -s "$FLOW_PREDECESSOR_FIXTURE_OUT/flow.sema"
              test -s "$FLOW_PREDECESSOR_FIXTURE_OUT/fixture.ids"
              runHook postCheck
            '';
          });
          flowNexus = "${members.flow-nexus}/bin/flow-nexus-daemon";
          # The checks beyond each member's, which join checks.default.
          extraChecks = {
            # nix/curriculum/Cargo.nix is what the base's crate2nix generates
            # from the pinned Curriculum, as cargo-nix proves Cargo.nix.
            curriculum-cargo-nix = context.cargoNixCheck {
              source = curriculum;
              cargoNix = ./nix/curriculum/Cargo.nix;
              crateHashes = ./nix/curriculum/crate-hashes.json;
            };
            fmt = pkgs.runCommand "flow-fmt" { nativeBuildInputs = [ toolchain ]; } ''
              cp -r ${rustSource} source
              chmod -R u+w source
              cd source
              export CARGO_HOME="$NIX_BUILD_TOP/cargo-home"
              cargo fmt --all --check
              touch "$out"
            '';
            # The capsule runner's own logic that runs no capsule: where a run
            # keeps its witnesses and what it copies there before teardown,
            # and its waits on their events (inotify, pidfd, a socket). The
            # tests run the workspace's own binaries (the invocation
            # composers), which the unconfigured default lacks.
            capsule-witnesses =
              pkgs.runCommand "flow-capsule-witnesses"
                {
                  nativeBuildInputs = [
                    pkgs.babashka
                    pkgs.inotify-tools
                    pkgs.util-linux
                  ];
                }
                ''
                  bb ${./capsule/flow-capsule-test.clj} ${./capsule/flow-capsule.clj} ${workspaceBinaries} > result.txt \
                    || { cat result.txt; exit 1; }
                  cat result.txt
                  cp result.txt "$out"
                '';
            # `fn main()` is the only production free function, and every
            # production method lives in a trait.
            no-free-functions = pkgs.runCommand "flow-no-free-functions" { src = lawSource; } (
              builtins.readFile ./checks/no-free-functions.sh
            );
            no-inherent-methods = pkgs.runCommand "flow-no-inherent-methods" { src = lawSource; } (
              builtins.readFile ./checks/no-inherent-methods.sh
            );
            # The accepted 0.53.2's real Memory, evolved and served by this
            # Flow Nexus, which refuses a malformed policy without writing.
            request-id-migration-process =
              processCheck "request-id-migration-process"
                "store::request_id_migration_process::actual_predecessor_memory_evolves_serves_and_refuses_malformed_policy_without_writing"
                ''
                  export FLOW_MIGRATION_PREDECESSOR_BIN="${migrationPredecessorPackage}/bin/flow-nexus"
                  export FLOW_MIGRATION_PREDECESSOR_STORE="${migrationPredecessorFixture}/share/request-id-predecessor"
                  export FLOW_MIGRATION_CURRENT_BIN="${flowNexus}"
                  export FLOW_MIGRATION_PREDECESSOR_REV="${predecessorRevision}"
                  export FLOW_MIGRATION_CURRENT_REV="${sourceRevision}"
                  export FLOW_MIGRATION_SOURCE_REV="${sourceRevision}"
                  sha256sum "$FLOW_MIGRATION_PREDECESSOR_BIN" "$FLOW_MIGRATION_CURRENT_BIN"
                '';
            flow-request-id-immutable-contract-pins =
              pkgs.runCommand "flow-request-id-immutable-contract-pins" { nativeBuildInputs = [ pkgs.gnugrep ]; }
                ''
                  grep -F 'rev = "37e99d3"' ${./Cargo.toml}
                  grep -F 'rev = "ac7db8a44c78c77df373e80fdf7d4d9ca5ad3048"' ${./Cargo.toml}
                  grep -F 'signal-flow?rev=37e99d3#37e99d39128a1554f4873d541a1b5d50545667e4' ${./Cargo.lock}
                  grep -F 'meta-signal-flow?rev=ac7db8a44c78c77df373e80fdf7d4d9ca5ad3048#ac7db8a44c78c77df373e80fdf7d4d9ca5ad3048' ${./Cargo.lock}
                  touch "$out"
                '';

          };
          # The pinned Curriculum, built per crate on the same base.
          curriculumCrates = import ./nix/curriculum/Cargo.nix {
            inherit pkgs;
            inherit (context) buildRustCrateForPkgs;
            defaultCrateOverrides = context.crateOverrides // {
              curriculum =
                attributes: (context.crateOverrides.curriculum or (_: { })) attributes // { src = curriculum; };
            };
            rootFeatures = [
              "default"
              "datom"
            ];
          };
        in
        {
          inherit
            pkgs
            toolchain
            workspace
            members
            commands
            testBuilds
            findTest
            binaries
            crateCheck
            processCheck
            checkEnvironments
            memberChecks
            workspaceBinaries
            lintFeatures
            rustSource
            lawSource
            ;
          crate2nix = context.crate2nix;
          curriculum = curriculumCrates.rootCrate.build;
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
            inherit lib version;
            inherit (context) pkgs processCheck binaries;
            flowNexus = context.members.flow-nexus;
            members = lib.attrValues (removeAttrs context.commands [ "flow-nexus" ]);
            memberChecks = lib.attrValues context.memberChecks;
          };
        }
      );

      packages = forSystems (
        system:
        let
          context = mkContext system;
          pkgs = context.pkgs;
        in
        context.members
        // {
          # Intentionally the last unconfigured artifact. A 0.43 package
          # is built only through lib.<system>.mkFlowPackage with an explicit
          # defaultConfiguration; production has supplied none yet.
          default = unconfigured-flow.packages.${system}.default;
          # The flow-nexus library test executable, from its test build, so a
          # test kept out of every check (one that needs a store no repository
          # holds, named at run time) can be run by name where a build runs.
          store-proof = pkgs.runCommand "flow-nexus-store-proof" { } ''
            ${context.findTest "binary" context.testBuilds.flow-nexus
              "store::request_id_migration_process::actual_predecessor_memory_evolves_serves_and_refuses_malformed_policy_without_writing"
            }
            mkdir -p "$out/bin"
            cp "$binary" "$out/bin/flow-nexus-store-proof"
          '';
          # The Curriculum Nexus and CLI of the Curriculum revision Flow
          # pins, as the capsule runs them.
          curriculum = context.curriculum;
          # The capsule test: a Babashka script and the Configure datoms it
          # applies, kept side by side, with the Curriculum it runs. It is
          # given the flow package to run. Its waits answer events through
          # inotifywait (inotify) and util-linux's waitpid (pidfd), named
          # to it by its launcher.
          flow-capsule = pkgs.runCommand "flow-capsule" { } ''
            mkdir -p "$out/libexec/flow-capsule" "$out/bin"
            ln -s ${self.packages.${system}.curriculum} "$out/libexec/flow-capsule/curriculum"
            {
              echo "#!${pkgs.babashka}/bin/bb"
              cat ${./capsule/flow-capsule.clj}
            } > "$out/libexec/flow-capsule/flow-capsule"
            chmod +x "$out/libexec/flow-capsule/flow-capsule"
            cp ${./capsule/configure.datom} "$out/libexec/flow-capsule/configure.datom"
            {
              echo "#!${pkgs.runtimeShell}"
              echo "export FLOW_CAPSULE_INOTIFYWAIT=${pkgs.inotify-tools}/bin/inotifywait"
              echo "export FLOW_CAPSULE_WAITPID=${lib.getExe' pkgs.util-linux "waitpid"}"
              echo "exec $out/libexec/flow-capsule/flow-capsule \"\$@\""
            } > "$out/bin/flow-capsule"
            chmod +x "$out/bin/flow-capsule"
          '';
        }
      );

      apps = forSystems (system: {
        flow-capsule = {
          type = "app";
          program = "${self.packages.${system}.flow-capsule}/bin/flow-capsule";
        };
      });

      # Every check, the base's cargo-nix with the ones above; checks.default
      # is all of them, so a green default is the whole gate.
      checks = forSystems (
        system:
        (mkContext system).workspace.checks
        // {
          # The package a deployment builds: the executable with a default
          # configuration composed into its Rust, so a contract change that
          # breaks that composition fails here and not at a deployment.
          configured-package = self.lib.${system}.mkFlowPackage {
            defaultConfiguration = {
              sourceRoot = "/home/example/primary";
              runtimeDirectory = "/run/user/1000/nexus/main";
              stableCodex = {
                clientPath = "/nix/store/example-codex-stable-flow-client/bin/codex-stable-flow-client";
                home = "/home/example/.codex-next";
                controlSocketPath = "/home/example/.codex-next/app-server-control/app-server-control.sock";
                roster = [ "gpt-5.6-sol" ];
              };
              nextCodex = {
                clientPath = "/nix/store/example-codex-next-flow-client/bin/codex-next-flow-client";
                home = "/home/example/.codex-next-candidate";
                controlSocketPath = "/home/example/.codex-next-candidate/app-server-control/app-server-control.sock";
                roster = [ "gpt-6-astra" ];
              };
              harnessProfiles = [
                {
                  harnessKind = "Claude";
                  commandSigils = [ "/" ];
                  interruptKeys = [ "esc" ];
                  submitKeys = [ "enter" ];
                }
              ];
              metaAspects = [ "Psyche" ];
              presentationPrompt = null;
              herdrSession = "default";
            };
          };
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
              context.crate2nix
              context.pkgs.jujutsu
              context.pkgs.nix
            ];
          };
        }
      );
    };
}
