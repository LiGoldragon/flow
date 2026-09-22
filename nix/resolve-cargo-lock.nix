{ pkgs, src }:
pkgs.runCommand "flow-cargo-lock-resolution" {
  nativeBuildInputs = [ pkgs.cargo ];
} ''
  set -euo pipefail
  cp -R --no-preserve=mode,ownership ${src}/. work
  chmod -R u+w work
  cd work
  cargo generate-lockfile
  install -Dm644 Cargo.lock "$out/Cargo.lock"
''
