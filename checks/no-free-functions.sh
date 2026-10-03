set -eu
. "$src/checks/production-rust.sh"

# `fn main()` is the one production free function the law allows, so the
# three binary entry points are the only lines excused here.
if strip_test_items $(production_rust_files) |
  grep -E ':[0-9]+:(pub(\([^)]*\))? )?((const|async|unsafe) )*fn ' |
  grep -v -E ':[0-9]+:fn main\('; then
  echo "production Rust must not use module-level free functions" >&2
  exit 1
fi
touch "$out"
