#!/usr/bin/env bash
set -euo pipefail

bash -n scripts/verify-acceptance-image.sh
go test ./scripts -run '^TestAcceptanceImageWorkflowContract$' -count=1
