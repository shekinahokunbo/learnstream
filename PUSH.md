# Push to GitHub, then deploy

```bash
cd learnstream
git init -b main
git add .
git commit -m "LearnStream: event-driven learning analytics pipeline on AWS"

gh repo create learnstream --public --source=. --push
# or, without the gh CLI:
#   git remote add origin git@github.com:shekinahokunbo/learnstream.git
#   git push -u origin main
```

## Deploy

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                      # run the suite before you spend money

aws configure               # if this account is not set up yet
cd infra
npx aws-cdk bootstrap       # once per account/region
npx aws-cdk deploy
cd ..
```

`cdk deploy` prints ApiUrl, QueueUrl, DlqUrl, TableName, BucketName, DashboardUrl.

## Measure

```bash
./scripts/measure.sh LearnStreamStack us-east-1
```

Takes about 8 minutes: a 3-minute load test, a 25x replay for the dedupe proof,
and a fault-injection drill that breaks the worker, fills the DLQ, restores it
and redrives. It prints the numbers to paste into `resume.tex`.

## CI deploys (optional)

1. In IAM, create an OIDC identity provider for `token.actions.githubusercontent.com`.
2. Create a role trusting `repo:shekinahokunbo/learnstream:ref:refs/heads/main`.
3. Save its ARN as the `AWS_DEPLOY_ROLE_ARN` repository secret.

## Tear down when you are done measuring

```bash
cd infra && npx aws-cdk destroy
```
