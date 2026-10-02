#!/usr/bin/env bash
# Exercise release state transitions with a fake gh; never contact GitHub.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
test_dir="$(mktemp -d)"
trap 'rm -rf -- "$test_dir"' EXIT
mkdir -p "$test_dir/bin"
cat > "$test_dir/bin/gh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$MOCK_LOG"
case "$1 $2" in
  'api --paginate')
    if [[ "$MOCK_FAIL" == api ]]; then exit 1; fi
    printf '%s\n' "$MOCK_RELEASES"
    ;;
  'release create') ;;
  'release upload')
    if [[ "$MOCK_FAIL" == upload ]]; then exit 1; fi
    ;;
  'release edit') ;;
  *) echo "Unexpected gh invocation: $*" >&2; exit 1 ;;
esac
MOCK
chmod +x "$test_dir/bin/gh"
export PATH="$test_dir/bin:$PATH"
export GH_REPO=example/codex-remote-android
export MOCK_LOG="$test_dir/gh.log"
export MOCK_RELEASES='[]'
export MOCK_FAIL=''

reset_case() {
  tag="${1:-v0.1.3}"
  artifact_dir="$test_dir/$tag"
  mkdir -p "$artifact_dir"
  printf 'test APK\n' > "$artifact_dir/codex-remote-$tag.apk"
  (cd "$artifact_dir" && sha256sum "codex-remote-$tag.apk" > SHA256SUMS)
  : > "$MOCK_LOG"
  MOCK_RELEASES='[]'
  MOCK_FAIL=''
}

publish() {
  bash "$script_dir/publish-release.sh" "$tag" "$artifact_dir" \
    > "$test_dir/stdout" 2> "$test_dir/stderr"
}

expect_failure() {
  if publish; then
    echo "Expected publishing to fail." >&2
    exit 1
  fi
}

reset_case
publish
grep -q '^release create v0.1.3 .*--verify-tag .*--draft .*--generate-notes .*--prerelease=false$' "$MOCK_LOG"
grep -q '^release upload v0.1.3 codex-remote-v0.1.3.apk SHA256SUMS --clobber$' "$MOCK_LOG"
test "$(tail -n 1 "$MOCK_LOG")" = 'release edit v0.1.3 --draft=false --prerelease=false'

reset_case v0.1.3-rc.1
publish
grep -q '^release create .*--prerelease=true$' "$MOCK_LOG"
test "$(tail -n 1 "$MOCK_LOG")" = 'release edit v0.1.3-rc.1 --draft=false --prerelease=true'

reset_case
# A matching draft on a later API page must be reused.
MOCK_RELEASES=$'[{"tag_name":"v9.0.0","draft":false}]\n[{"tag_name":"v0.1.3","draft":true}]'
publish
if grep -q '^release create ' "$MOCK_LOG"; then exit 1; fi
grep -q '^release upload ' "$MOCK_LOG"
grep -q '^release edit ' "$MOCK_LOG"

reset_case
MOCK_RELEASES='[{"tag_name":"v0.1.3","draft":false}]'
expect_failure
if grep -q '^release ' "$MOCK_LOG"; then exit 1; fi

reset_case
MOCK_FAIL=api
expect_failure
if grep -q '^release ' "$MOCK_LOG"; then exit 1; fi

reset_case
MOCK_FAIL=upload
expect_failure
grep -q '^release create ' "$MOCK_LOG"
if grep -q '^release edit ' "$MOCK_LOG"; then exit 1; fi
# Retry after the external upload problem is resolved.
MOCK_FAIL=''
MOCK_RELEASES='[{"tag_name":"v0.1.3","draft":true}]'
: > "$MOCK_LOG"
publish
if grep -q '^release create ' "$MOCK_LOG"; then exit 1; fi
grep -q '^release edit ' "$MOCK_LOG"

reset_case
printf 'tampered\n' >> "$artifact_dir/codex-remote-$tag.apk"
expect_failure
test ! -s "$MOCK_LOG"

reset_case invalid-tag
expect_failure
test ! -s "$MOCK_LOG"

echo 'Release publishing checks passed (9 scenarios).'
