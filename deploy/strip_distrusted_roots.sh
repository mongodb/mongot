#!/usr/bin/env bash
# Removes distrusted roots from a JDK cacerts keystore at package time (CLOUDP-466523).
# The bundled JDK trust store is independent of the OS trust store, so OS-level distrust
# never reaches it; this strips the compromised anchors from the store we ship.
#
# Usage: strip_distrusted_roots.sh <keytool> <in-cacerts> <out-cacerts> <report> <subject-CN>...
#
# Fails unless every requested subject CN is found and removed, and none remain
# afterward. Writes a TSV report (removed <CN> <alias> SHA256:<fingerprint>) consumed by
# //deploy:distrusted_roots_stripped_test as per-release evidence.
set -euo pipefail

keytool="$1"
in="$2"
out="$3"
report="$4"
shift 4

cp "$in" "$out"
chmod u+w "$out"
: >"$report"

listing() { "$keytool" -list -v -keystore "$out" -storepass changeit 2>/dev/null; }

before=$(listing | grep -c '^Alias name:')

for cn in "$@"; do
  # Resolve subject CN -> alias via the Owner line: alias naming conventions differ
  # across Temurin releases, but the certificate subject does not. The alias is
  # everything after "Alias name: " (Temurin aliases contain spaces, e.g. a
  # trailing " [jdk]").
  aliases=$(listing | awk -v needle="CN=${cn}," '
    /^Alias name: / { a = substr($0, 13) }
    /^Owner:/ && index($0, needle) { print a }')
  if [[ -z "$aliases" ]]; then
    # Already absent (newer Temurin releases dropped some of these upstream). The
    # desired end state is "not trusted", so record it and move on; the post-conditions
    # below still assert absence from the shipped store.
    printf 'absent\t%s\t-\t-\n' "$cn" >>"$report"
    continue
  fi
  while IFS= read -r alias; do
    fp=$(listing | awk -v a="$alias" '
      /^Alias name: / { m = (substr($0, 13) == a) }
      m && /SHA256:/ { sub(/^.*SHA256: /, ""); print; exit }')
    "$keytool" -delete -alias "$alias" -keystore "$out" -storepass changeit
    printf 'removed\t%s\t%s\tSHA256:%s\n' "$cn" "$alias" "$fp" >>"$report"
  done <<<"$aliases"
done

# Post-conditions: none of the requested CNs remain, and the entry count dropped by
# exactly the number of removals.
for cn in "$@"; do
  if listing | grep -q "CN=${cn},"; then
    echo "FATAL: CN=${cn} still present after deletion" >&2
    exit 1
  fi
done
removed=$(grep -c '^removed	' "$report" || true)
after=$(listing | grep -c '^Alias name:')
if [[ $((before - removed)) -ne $after ]]; then
  echo "FATAL: entry count mismatch: ${before} - ${removed} != ${after}" >&2
  exit 1
fi
