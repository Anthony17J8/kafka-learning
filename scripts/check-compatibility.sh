#!/usr/bin/env bash
set -euo pipefail
SR=http://localhost:8081
SUBJ=orders-avro-value
DIR=04-serialization-schema-registry/candidates   # папка с вариантами схем

for mode in BACKWARD FORWARD FULL; do
  curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"compatibility\": \"$mode\"}" "$SR/config/$SUBJ" > /dev/null
  echo "=== $mode ==="
  for f in "$DIR"/*.avsc; do
    result=$(jq -n --rawfile s "$f" '{schema: $s}' \
      | curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
          --data @- "$SR/compatibility/subjects/$SUBJ/versions/latest?verbose=true")
    ok=$(echo "$result" | jq -r '.is_compatible // "ERROR"')
    err=$(echo "$result" | jq -r '.messages[0] // ""' | grep -o "errorType:'[^']*'" | head -1 || true)
    printf "%-40s %-6s %s\n" "$(basename "$f")" "$ok" "$err"
  done
  echo
done
