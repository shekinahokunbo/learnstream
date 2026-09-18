#!/usr/bin/env bash
# Write a clean ~/.aws/credentials and verify it.
#   ./scripts/aws_setup.sh
set -euo pipefail

REGION="${REGION:-us-east-1}"
PY="${PY:-python3}"
[ -x .venv/bin/python3 ] && PY=".venv/bin/python3"

echo
echo "This rewrites ~/.aws/credentials from scratch (the old file is backed up)."
echo "Both values are shown as you type so you can check them before saving."
echo "You need the Access key ID (starts with AKIA, 20 characters) and the"
echo "Secret access key (40 characters). If you no longer have the secret,"
echo "create a new access key in IAM first — a secret is only shown once."
echo

read -r -p "AWS Access Key ID: " KEY_ID
read -r -p "AWS Secret Access Key: " SECRET

# Strip spaces, tabs, newlines and any stray quotes from a sloppy paste.
KEY_ID=$(printf '%s' "$KEY_ID" | tr -d '[:space:]"'"'")
SECRET=$(printf '%s' "$SECRET" | tr -d '[:space:]"'"'")

echo
echo "  key id : ${#KEY_ID} characters (${KEY_ID:0:4}...${KEY_ID: -4})"
echo "  secret : ${#SECRET} characters (${SECRET:0:3}...${SECRET: -3})"

FAIL=0
if [ ${#KEY_ID} -ne 20 ]; then
  echo "  !! An access key ID is normally 20 characters. Yours is ${#KEY_ID}."; FAIL=1
fi
if [ ${#SECRET} -ne 40 ]; then
  echo "  !! A secret access key is normally 40 characters. Yours is ${#SECRET}."
  echo "     120 characters means it was pasted three times. Run this again and paste once."
  FAIL=1
fi
[ $FAIL -eq 1 ] && { echo; echo "Nothing was saved. Fix the values and re-run."; exit 1; }

mkdir -p ~/.aws
[ -f ~/.aws/credentials ] && cp ~/.aws/credentials ~/.aws/credentials.bak.$(date +%s)

cat > ~/.aws/credentials <<EOF
[default]
aws_access_key_id = ${KEY_ID}
aws_secret_access_key = ${SECRET}
EOF
chmod 600 ~/.aws/credentials

cat > ~/.aws/config <<EOF
[default]
region = ${REGION}
output = json
EOF

echo
echo "Saved. Verifying with AWS..."
$PY - <<'PYEOF'
import sys
try:
    import boto3
except ImportError:
    print("  (boto3 not installed yet — run ./run_it.sh and it will verify there)")
    sys.exit(0)
try:
    who = boto3.client("sts").get_caller_identity()
    print(f"  OK — authenticated as account {who['Account']}")
    print(f"  identity: {who['Arn']}")
except Exception as exc:
    print(f"  FAILED: {exc}")
    print("  If this says SignatureDoesNotMatch, the secret is wrong: create a new")
    print("  access key in IAM and run this script again.")
    sys.exit(1)
PYEOF
echo
echo "Now run:  ./run_it.sh"
