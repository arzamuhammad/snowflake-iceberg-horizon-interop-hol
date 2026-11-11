#!/usr/bin/env bash
# Lab 4b - Verify a PAT against the Horizon IRC endpoint before configuring any engine.
#   export SNOWFLAKE_ACCOUNT=<org>-<account> SNOWFLAKE_PAT=<PAT>
#   ./scripts/verify_horizon_irc.sh GROUP_DBX_READER GROUP_LAKEHOUSE
set -euo pipefail
ROLE="${1:?role}"; DB="${2:?database}"
BASE="https://${SNOWFLAKE_ACCOUNT:?}.snowflakecomputing.com/polaris/api/catalog"

echo "1) Exchange PAT for an access token (scope session:role:${ROLE})"
TOKEN=$(curl -sf -X POST "${BASE}/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=client_credentials' \
  --data-urlencode "scope=session:role:${ROLE}" \
  --data-urlencode "client_secret=${SNOWFLAKE_PAT:?}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
echo "   ok"

echo "2) Catalog config for warehouse=${DB}"
curl -sf "${BASE}/v1/config?warehouse=${DB}" -H "Authorization: Bearer ${TOKEN}"; echo

echo "3) Namespaces visible to ${ROLE}"
curl -sf "${BASE}/v1/${DB}/namespaces" -H "Authorization: Bearer ${TOKEN}"; echo
