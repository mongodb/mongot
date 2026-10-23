#!/usr/bin/env bash
# Asserts the strip report lists exactly the CLOUDP-466523 distrusted roots, each with a
# fingerprint: the per-release evidence that the shipped JDK trust store no longer
# contains them. The keystore-level post-conditions (roots absent, entry count) are
# enforced by strip_distrusted_roots.sh itself, which fails the build otherwise.
set -euo pipefail

report="$1"

fail() {
  echo "FAIL: $1" >&2
  echo "--- report ---" >&2
  cat "$report" >&2
  exit 1
}

lines=$(wc -l <"$report" | tr -d ' ')
[[ "$lines" == "2" ]] || fail "expected exactly 2 report rows, got ${lines}"

# Each distrusted root is either removed here (with a fingerprint for the audit trail) or
# already absent from the upstream store; the strip script fails the build if either CN
# remains in the shipped store.
for cn in "AAA Certificate Services" "emSign ECC Root CA - G3"; do
  if grep -q "^removed	${cn}	" "$report"; then
    grep "^removed	${cn}	" "$report" | grep -q 'SHA256:..' \
      || fail "removal record for CN=${cn} lacks a fingerprint"
  elif ! grep -q "^absent	${cn}	" "$report"; then
    fail "no removal/absent record for CN=${cn}"
  fi
done

echo PASS
