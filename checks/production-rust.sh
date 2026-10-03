# Shared preamble for the two trait-law checks.
#
# flow keeps production Rust in its four workspace members' `src/`. `tests/`
# is not production source, and neither are the `#[cfg(test)]` items inside
# production files nor the files that only a `#[cfg(test)]` module declares
# (`flow-nexus/src/tests/` and `flow-nexus/src/fixture_executable.rs`), so the
# scan reads each production file with its `#[cfg(test)]` items removed and
# leaves the test-only files out. The stripper drops a `#[cfg(test)]` item
# whole: the attribute line, and then either the rest of a statement ending in
# `;` or the brace-balanced block that follows. String literals, character
# literals holding a brace, and line comments are blanked before braces are
# counted so that text cannot unbalance the scan.
production_sources() {
  printf '%s\n' \
    "$src/crates/flow-defaults/src" \
    "$src/crates/flow-nexus/src" \
    "$src/crates/flow/src" \
    "$src/crates/flow-meta/src"
}

strip_test_items() {
  awk '
    function clean(s) {
      gsub(/\\./, "", s)
      gsub(/"[^"]*"/, "", s)
      gsub(/'"'"'[{}]'"'"'/, "", s)
      sub(/\/\/.*/, "", s)
      return s
    }
    {
      if (skip == 0 && $0 ~ /^[ \t]*#\[cfg\((test\)|all\(test|any\(test)/) {
        skip = 1; depth = 0; opened = 0; next
      }
      if (skip == 1) {
        line = clean($0)
        n = gsub(/\{/, "{", line); m = gsub(/\}/, "}", line)
        depth += n - m
        if (opened == 0 && depth > 0) { opened = 1 }
        if (opened == 1 && depth <= 0) { skip = 0 }
        else if (opened == 0 && line ~ /;[ \t]*$/) { skip = 0 }
        next
      }
      print FILENAME ":" FNR ":" $0
    }
  ' "$@"
}

production_rust_files() {
  find $(production_sources) -name '*.rs' -type f \
    -not -path '*/flow-nexus/src/tests/*' \
    -not -path '*/flow-nexus/src/fixture_executable.rs' | sort
}
