#!/usr/bin/env bash
# Coverage gate: measures workspace line/function coverage via cargo-llvm-cov
# and fails if either drops below the configured floor. It is a regression
# guard, not a complete-coverage promise: thresholds sit below the current
# measured baseline (lines ~92%, functions ~88%) so scope growth or dead-code
# removal doesn't trip it, while a real coverage regression fails the pipeline.
#
# Usage: ./deploy/coverage.sh                    # measure + enforce defaults
#        DOCSQL_COV_LINE_MIN=90 DOCSQL_COV_FUNC_MIN=85 ./deploy/coverage.sh
set -euo pipefail

LINE_MIN="${DOCSQL_COV_LINE_MIN:-85}"
FUNC_MIN="${DOCSQL_COV_FUNC_MIN:-80}"
LCOV="$(mktemp "${TMPDIR:-/tmp}/docsql-lcov.XXXXXX")"
trap 'rm -f "$LCOV"' EXIT

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "coverage: installing cargo-llvm-cov (rustup llvm-tools-preview + cargo install)..."
  rustup component add llvm-tools-preview
  cargo install cargo-llvm-cov --locked
fi

echo "coverage: measuring workspace (line >= ${LINE_MIN}%, func >= ${FUNC_MIN}%)..."
cargo llvm-cov --workspace --lcov --output-path "$LCOV"

line="$(awk -F: '/^LF:/{lf+=$2}/^LH:/{lh+=$2}END{if (lf>0) printf "%.1f", 100*lh/lf; else print 0}' "$LCOV")"
func="$(awk -F: '/^FNF:/{fn+=$2}/^FNH:/{fh+=$2}END{if (fn>0) printf "%.1f", 100*fh/fn; else print 0}' "$LCOV")"
echo "coverage: lines=${line}% functions=${func}%"

fail=0
if awk -v a="$line" -v b="$LINE_MIN" 'BEGIN{exit !(a<b)}'; then
  echo "coverage: line coverage ${line}% is below the ${LINE_MIN}% floor" >&2
  fail=1
fi
if awk -v a="$func" -v b="$FUNC_MIN" 'BEGIN{exit !(a<b)}'; then
  echo "coverage: function coverage ${func}% is below the ${FUNC_MIN}% floor" >&2
  fail=1
fi
exit $fail