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
            version = "0.10.0";
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
