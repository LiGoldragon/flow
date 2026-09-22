{ pkgs, src }:
pkgs.runCommand "flow-cargo-lock-resolution" {
  nativeBuildInputs = [ pkgs.cargo pkgs.python3 ];
} ''
  set -euo pipefail
  cp -R --no-preserve=mode,ownership ${src}/. work
  chmod -R u+w work
  cd work
  cargo generate-lockfile
  install -Dm644 Cargo.lock "$out/Cargo.lock"
  python3 - <<'PYTHON' > "$out/git-source-keys.txt"
import tomllib
with open("Cargo.lock", "rb") as lock:
    packages = tomllib.load(lock)["package"]
for source in sorted({package.get("source", "") for package in packages if package.get("source", "").startswith("git+")}):
    print(source)
PYTHON
''
