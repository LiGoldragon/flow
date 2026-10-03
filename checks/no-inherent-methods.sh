set -eu
. "$src/checks/production-rust.sh"

if strip_test_items $(production_rust_files) |
  grep -E ':[0-9]+:[[:space:]]*impl(<[^>]*>)?[[:space:]]' |
  grep -v ' for ' | grep -E '\{[[:space:]]*$'; then
  echo "production Rust must home behavior in traits" >&2
  exit 1
fi
touch "$out"
