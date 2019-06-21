gen-docker:
	docker build \
		-f workivabuild.Dockerfile \
		-t workivadocker/eva .

gen-docker-no-tests:
	docker build \
		--build-arg SKIP_TESTS=true \
		-f workivabuild.Dockerfile \
		-t workivadocker/eva .

run-docker:
	./scripts/ci/pull_composes.sh
	docker-compose -f docker/docker-compose.yml \
		-f compose_remote/local-compose-eva-catalog.yml \
		-f docker/docker-compose.override.yml up -d

stop-docker:
	docker-compose -f docker/docker-compose.yml \
		-f compose_remote/local-compose-eva-catalog.yml \
		-f docker/docker-compose.override.yml down

docker-logs:
	docker-compose -f docker-compose.yml \
		-f compose_remote/local-compose-eva-catalog.yml \
		-f docker/docker-compose.override.yml logs

repl: run-docker ## Starts a Clojure REPL in the docker network
	docker-compose -f docker/docker-compose.repl.yml run --rm lein with-profile +dev repl

transactor-docker:
	docker-compose -f docker/docker-compose.transactor.yml run --rm lein with-profile +dev repl

update-tocs:
	./.circleci/scripts/update-tocs.sh
