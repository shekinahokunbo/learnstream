.PHONY: install test lint synth deploy destroy loadtest

install:
	pip install -r requirements-dev.txt

test:
	pytest --cov=src --cov-report=term-missing

lint:
	ruff check src infra tests

synth:
	cd infra && cdk synth

deploy:
	cd infra && cdk deploy --require-approval never

destroy:
	cd infra && cdk destroy --force

loadtest:
	@test -n "$(API_URL)" || (echo "usage: make loadtest API_URL=https://..." && exit 1)
	locust -f loadtest/locustfile.py --host $(API_URL) \
		--users 100 --spawn-rate 10 --run-time 5m --headless --csv results/run
