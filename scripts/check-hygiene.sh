#!/usr/bin/env bash
#
# Fails if anything organisation-specific has leaked into the tree.
#
# shard4j is a product; the projects that run it are consumers. No hostname, registry,
# cluster, namespace, account id or organisation name belongs in code, tests, fixtures or
# comments here. Three lines of grep, and the only thing that will still be true in a year.
#
# Allow-listed: README.md, which links the design issue and names the first known user,
# and this script, which has to contain the patterns it looks for.

set -euo pipefail

cd "$(dirname "$0")/.."

PATTERNS=(
  'sqrl\.site'
  'staging-sqrlcloud'
  'datasqrl'
  'cloud-compilation'
  'amazonaws\.com'
  '\.ecr\.'
  '[^0-9][0-9]{12}[^0-9]'
)

if git rev-parse --git-dir >/dev/null 2>&1; then
  mapfile -t FILES < <(git ls-files)
else
  mapfile -t FILES < <(find . -type f -not -path './.git/*' -not -path '*/target/*')
fi

ALLOWED='^(\./)?(README\.md|scripts/check-hygiene\.sh)$'

CANDIDATES=()
for f in "${FILES[@]}"; do
  [[ "$f" =~ $ALLOWED ]] && continue
  CANDIDATES+=("$f")
done

status=0
for pattern in "${PATTERNS[@]}"; do
  if grep -rInE --binary-files=without-match "$pattern" "${CANDIDATES[@]}"; then
    echo "HYGIENE FAILURE: pattern /$pattern/ found above." >&2
    echo "Nothing organisation-specific belongs in this repository. See AGENTS.md." >&2
    status=1
  fi
done

if [ "$status" -eq 0 ]; then
  echo "Hygiene gate passed: ${#CANDIDATES[@]} files scanned, no organisation-specific strings."
fi

exit "$status"
