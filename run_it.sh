#!/usr/bin/env bash
# One command, start to finish. Run in macOS Terminal from the learnstream folder:
#
#   ./run_it.sh
#
# No Homebrew and no AWS CLI needed: everything installs into a local venv.
set -euo pipefail

STACK="${STACK:-LearnStreamStack}"
REGION="${REGION:-us-east-1}"

say() { printf "\n\033[1m==> %s\033[0m\n" "$1"; }

say "0/6 prerequisites"
for c in python3 node npm; do
  command -v "$c" >/dev/null || { echo "missing: $c"; exit 1; }
done
echo "ok: $(python3 --version), node $(node --version)"

say "1/6 smoke test (no dependencies)"
python3 tests/smoke_stdlib.py

say "2/6 virtual environment"
[ -d .venv ] || python3 -m venv .venv
source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements-dev.txt
echo "ok: dependencies installed into .venv"

say "3/6 AWS credentials"
if ! python3 -c "
import boto3, sys
try:
    boto3.client('sts', region_name='$REGION').get_caller_identity()
except Exception as exc:
    print(exc); sys.exit(1)
" >/dev/null 2>&1; then
  echo "No working AWS credentials found."
  echo
  echo "Run this, then start run_it.sh again:"
  echo "    ./scripts/aws_setup.sh"
  echo
  echo "It writes ~/.aws/credentials cleanly and checks the values before saving."
  exit 1
fi
python3 -c "
import boto3
who = boto3.client('sts', region_name='$REGION').get_caller_identity()
print('  authenticated as account', who['Account'])
"
ACCOUNT=$(python3 -c "import boto3;print(boto3.client('sts',region_name='$REGION').get_caller_identity()['Account'])")

say "4/6 full test suite"
pytest --cov=src --cov-report=term || {
  echo "Tests failed. Nothing has been created in AWS. Paste the failure to Claude."; exit 1; }

say "5/6 deploy (3-6 minutes; first run also bootstraps)"
cd infra
npx --yes aws-cdk@2 bootstrap "aws://${ACCOUNT}/${REGION}"
npx --yes aws-cdk@2 deploy --require-approval never
cd ..

say "6/6 measure (about 8 minutes)"
mkdir -p results
python3 scripts/measure.py --stack "$STACK" --region "$REGION"

cat <<'DONE'

Numbers are saved in results/summary.txt. Paste that block to Claude.

When you are done measuring, tear it down so nothing keeps billing:
    cd infra && npx aws-cdk@2 destroy
DONE
