#!/usr/bin/env bash
# Owner-attended preparation only. No setup, administrator or runtime commands.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo="$(cd -- "$script_dir/.." && pwd -P)"
root="$HOME/.local/share/wefty-proof"
for tool in python3 go git gh jq npm limactl; do
  command -v "$tool" >/dev/null
done
# Read only the version; never start or inspect an instance during preparation.
lima_version="$(limactl --version)"
python3 - "$lima_version" <<'PYVERSION'
import re, sys
match = re.search(r'\b(\d+)\.(\d+)\.(\d+)([^\s]*)', sys.argv[1])
if (not match or tuple(map(int, match.group(1, 2, 3))) < (2, 2, 0)
        or match.group(4).startswith('-')):
    raise SystemExit('Stage A requires stable Lima 2.2.0 or newer; found: ' + sys.argv[1])
print('Stage A prerequisite: ' + sys.argv[1])
PYVERSION
# The orchestrator initialized the one baseline and runs guard-watch separately.
python3 "$script_dir/measure-per-user-proof.py" guard-check
mkdir -p "$root/source-bin" "$root/assets" "$root/tools"
cd "$repo"
version="$(git rev-parse HEAD)"
for program in wefty wefty-l1 wefty-l3 wefty-agent; do
  python3 "$script_dir/measure-per-user-proof.py" guard-check
  GOFLAGS=-p=1 GOMAXPROCS=2 go build -ldflags "-X main.version=$version" \
    -o "$root/source-bin/$program" "./cmd/$program"
done
python3 "$script_dir/measure-per-user-proof.py" guard-check
GOFLAGS=-p=1 GOMAXPROCS=2 GOOS=linux go build -ldflags "-X main.version=$version" \
  -o "$root/assets/wefty-agent-linux" ./cmd/wefty-agent
python3 "$script_dir/measure-per-user-proof.py" guard-check
gh run list --repo Derek-X-Wang/wefty --workflow computer-seats.yml --branch main \
  --event push --status success --limit 1 --json databaseId,headSha > "$root/assets/publication.json"
publication_run="$(jq -er '.[0].databaseId' "$root/assets/publication.json")"
publication_commit="$(jq -er '.[0].headSha' "$root/assets/publication.json")"
gh run download "$publication_run" --repo Derek-X-Wang/wefty \
  --name "wefty-computer-default-$publication_commit" --dir "$root/assets/image"
image="$(cat "$root/assets/image/default-computer-reference.txt")"
bash "$script_dir/verify-acceptance-image.sh" \
  --archive "$root/assets/image/default-computer.oci.tar" \
  --expected-top-digest "${image##*@}" --expected-platform linux/amd64 \
  --expected-platform linux/arm64 --receipt-output "$root/assets/image/verified.json"
python3 - "$root" "$version" "$image" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]); helper = root / 'assets/wefty-agent-linux'
reference, digest = sys.argv[3].rsplit('@', 1)
config = dict(linux_helper=str(helper), helper_version=sys.argv[2],
              helper_checksum='sha256:' + hashlib.sha256(helper.read_bytes()).hexdigest(),
              probe_archive=str(root / 'assets/image/default-computer.oci.tar'),
              probe_reference=reference, probe_digest=digest)
(root / 'assets/oci-assets.json').write_text(json.dumps(config, indent=2) + '\n')
(root / 'assets/source-revision.txt').write_text(sys.argv[2] + '\n')
PY
python3 "$script_dir/measure-per-user-proof.py" guard-check
mkdir -p "$root/assets/browser-kit"
npm install --prefix "$root/assets/browser-kit" --ignore-scripts --no-audit \
  --no-fund --save-exact playwright@1.56.1
cp "$script_dir/stage-a-browser.html" "$script_dir/stage-a-playwright.cjs" "$root/assets/"
python3 "$script_dir/measure-per-user-proof.py" guard-check
