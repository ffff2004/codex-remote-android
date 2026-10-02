#!/usr/bin/env bash
# Publish verified APKs, resuming an existing draft after an upload failure.
set -euo pipefail

tag="${1:?Usage: publish-release.sh TAG ARTIFACT_DIRECTORY}"
artifact_dir="${2:?Usage: publish-release.sh TAG ARTIFACT_DIRECTORY}"
: "${GH_REPO:?GH_REPO must identify the target repository}"
if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]]; then
  echo "Invalid release tag: $tag" >&2
  exit 1
fi

cd "$artifact_dir"
test -s "codex-remote-$tag.apk"
sha256sum --check SHA256SUMS

# Query failures must abort, rather than being mistaken for a missing release.
# Pagination also finds older drafts when resuming a failed workflow.
release="$(gh api --paginate "repos/$GH_REPO/releases?per_page=100" |
  jq -sc --arg tag "$tag" '[.[][] | select(.tag_name == $tag)] | .[0] // null')"
if [[ "$release" != null ]] && ! jq -e '.draft == true' <<< "$release" > /dev/null; then
  echo "Release $tag is already published; refusing to replace its assets." >&2
  exit 1
fi

prerelease=false
if [[ "$tag" == *-* ]]; then
  prerelease=true
fi
if [[ "$release" == null ]]; then
  gh release create "$tag" --verify-tag --draft --title "$tag" --generate-notes \
    "--prerelease=$prerelease"
fi

# Only draft assets may be replaced. Publish after both uploads complete.
gh release upload "$tag" "codex-remote-$tag.apk" SHA256SUMS --clobber
gh release edit "$tag" --draft=false "--prerelease=$prerelease"
