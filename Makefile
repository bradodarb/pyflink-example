## Better defaults for make (thanks https://tech.davis-hansson.com/p/make/)
SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
.SECONDEXPANSION:

VENV:=. ./.venv/bin/activate

ifndef app
app := app
endif

ifndef delay_ms
delay_ms := 100
endif

ifndef loops
loops := 1
endif

.PHONY: fresh
fresh:
	rm -rf .venv
	virtualenv .venv
	@make deps
	@echo Activate the virtual environment by typing 'source ./.venv/bin/activate'

.PHONY: package_aws
package_aws: clean jar
	DOCKER_BUILDKIT=1 docker build --build-arg APPLICATION_FILE=${application} --build-arg DIST_ARTIFACT_NAME=pyflink-example -t pyflink/example -f ./build/aws/MAF.Dockerfile --output ./dist .

.PHONY: deploy_aws
deploy_aws:
	aws s3 cp ./dist/*.zip s3://pyflink-bucket/releases/

.PHONY: package_local
package_local: clean jar
	DOCKER_BUILDKIT=1 docker build -t pyflink/dist -f ./build/local/Dockerfile --output ./dist .
	mkdir -p ./.local/flink/dist
	rm -rf ./.local/flink/dist/*
	cp -a ./dist/. ./.local/flink/dist/


.PHONY: jar
jar:
	cd lib && DOCKER_BUILDKIT=1 docker build -t pyflink/jarbundler -f ./maven.Dockerfile --output ./bin .

clean:
	rm -rf ./dist


deps:
	$(VENV)
	pip install -r ./requirements.txt


build: package_aws

.PHONY: services
services: package_local
	docker compose up --build -d

.PHONY: clear_jobs
clear_jobs:
	docker compose exec jobmanager bash -c '\
		for jid in $$(./bin/flink list -a 2>/dev/null | grep -oE "[0-9a-f]{32}" | sort -u); do \
			echo "Cancelling $$jid"; \
			./bin/flink cancel $$jid 2>/dev/null || true; \
		done; \
		echo "Done"'

.PHONY: run
run:
	docker compose exec jobmanager ./bin/flink run -py /opt/develop/${app}.py -pyfs /opt/develop/py_deps.zip -j /opt/develop/lib/pyflink-services-1.0.jar


.PHONY: generate
generate:
	python generators/kinesis_producer.py --file generators/sensors.json --stream input_stream --endpoint http://localhost:4566 --delay ${delay_ms} --loops ${loops}

.PHONY: test_put_kinesis
test_put_kinesis:
	export AWS_ACCESS_KEY_ID="test"
	export AWS_SECRET_ACCESS_KEY="test"
	export AWS_DEFAULT_REGION="us-east-1"
	$(eval DATA = $(shell echo $(testdata) | base64))
	aws kinesis put-record --stream-name input_stream --partition-key 123 --data $(DATA) --endpoint-url http://localhost:4566

.PHONY: test_get_kinesis
test_get_kinesis:
	$(eval SHARD_ITERATOR = $(shell aws kinesis get-shard-iterator --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --stream-name output_stream --query 'ShardIterator' --endpoint-url http://localhost:4566))
	$(info ${SHARD_ITERATOR})
	# read the records, use `jq` to grab the data of the first record, and base64 decode it
	aws kinesis get-records --shard-iterator $(SHARD_ITERATOR) --endpoint-url http://localhost:4566 | jq -r '.Records'


.PHONY: create_table_ddb
create_table_ddb:
	aws dynamodb create-table \
		--endpoint-url http://localhost:8000 \
		--table-name PyFlinkTestTable \
		--attribute-definitions AttributeName=id,AttributeType=S AttributeName=timestamp,AttributeType=S \
		--key-schema AttributeName=id,KeyType=HASH AttributeName=timestamp,KeyType=RANGE \
		--provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

.PHONY: remove_table_ddb
remove_table_ddb:
	aws dynamodb delete-table \
		--endpoint-url http://localhost:8000 \
		--table-name PyFlinkTestTable

.PHONY: scan_table_ddb
scan_table_ddb:
	aws dynamodb scan --table-name PyFlinkTestTable --endpoint-url http://localhost:8000