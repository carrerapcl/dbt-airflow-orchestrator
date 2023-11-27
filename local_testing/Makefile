.PHONY: build-airflow up setup check-docker

check-docker:
	@docker info > /dev/null 2>&1 || (echo "ERROR: Docker is not running. Please start Docker and try again."; exit 1)

build-airflow: check-docker
	docker build . -f Dockerfile --pull --tag my-airflow:0.0.1

up: check-docker
	docker-compose up

setup: build-airflow up