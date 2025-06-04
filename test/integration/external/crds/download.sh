#!/bin/bash

# Exit on any error
set -euo pipefail

# Usage message
usage() {
  echo "Usage: $0 <version>"
  echo "Example: $0 v0.6.1"
  exit 1
}

# Check argument count
if [ $# -ne 1 ]; then
    usage
fi

VERSION="$1"

URL="https://raw.githubusercontent.com/kubernetes-sigs/lws/${VERSION}/charts/lws/templates/crds/leaderworkerset.x-k8s.io_leaderworkersets.yaml"
DEST=$(basename "$URL")

curl -L --fail --silent --show-error "$URL" -o "$DEST"
# replace placeholders
sed -i.bak 's/{{[[:space:]]*\.Release\.Namespace[[:space:]]*}}/lws-system/g' "$DEST"
rm -f "$DEST.bak"
echo "Downloaded CRD to $DEST"
