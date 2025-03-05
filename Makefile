.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS += -e

CGW_IMG_ID := openlan-cgw-img
CGW_IMG_TAG := $(shell \
	if [[ `git status --porcelain --untracked-files=no` ]]; then \
		echo "`git rev-parse --short HEAD`-dirty"; \
	else \
		echo "`git rev-parse --short HEAD`"; \
	fi)
CGW_IMG_CONTAINER_NAME := "openlan_cgw"

CGW_BUILD_ENV_IMG_ID := openlan-cgw-build-env
CGW_BUILD_ENV_IMG_TAG := $(shell cat Dockerfile | sha1sum | awk '{print substr($$1,0,11);}')

CGW_BUILD_ENV_IMG_CONTAINER_NAME := "cgw_build_env"

.PHONY: all cgw-app cgw-build-env-img cgw-img stop clean run run_docker_services start-multi-cgw stop-multi-cgw run-tests

all: proxy-img start-multi-cgw
	@echo "uCentral CGW build app (container) done"

# Executed inside build-env
cgw-app:
	cargo build --target x86_64-unknown-linux-gnu --release

# Builds build-env image itself
cgw-build-env-img:
	@echo "Trying to build build-env-img, looking if exists.."
	@docker inspect --type=image ${CGW_BUILD_ENV_IMG_ID}:${CGW_BUILD_ENV_IMG_TAG} >/dev/null 2>&1 || \
		(echo "build-env-img doesn't exist, building..." && \
		docker build --file Dockerfile \
		--tag ${CGW_BUILD_ENV_IMG_ID}:${CGW_BUILD_ENV_IMG_TAG} \
		--target builder \
		.)
	@echo "build-env-img build done"

# Generates both build-env img as well as CGW result docker img
# Uses local FS / project dir for storing cache for build etc
cgw-img: stop cgw-build-env-img
	@docker run -it --name ${CGW_BUILD_ENV_IMG_CONTAINER_NAME} --network=host \
		${CGW_BUILD_ENV_IMG_ID}:${CGW_BUILD_ENV_IMG_TAG}
	@docker build --file Dockerfile \
		--build-arg="CGW_CONTAINER_BUILD_REV=${CGW_IMG_TAG}" \
		--tag ${CGW_IMG_ID}:${CGW_IMG_TAG} \
		--target cgw-img \
		.
	@echo Docker build done;

stop: stop-multi-cgw
	@echo "Stopping / removing container ${CGW_IMG_CONTAINER_NAME}"
	@docker stop ${CGW_IMG_CONTAINER_NAME} > /dev/null 2>&1 || true;
	@docker container rm ${CGW_IMG_CONTAINER_NAME} > /dev/null 2>&1 || true;
	@echo "Stopping / removing container ${CGW_BUILD_ENV_IMG_CONTAINER_NAME}"
	@docker stop ${CGW_BUILD_ENV_IMG_CONTAINER_NAME} > /dev/null 2>&1 || true;
	@docker container rm ${CGW_BUILD_ENV_IMG_CONTAINER_NAME} > /dev/null 2>&1 || true;

clean: stop
	@echo Cleaning build env and artifacts...
	@docker rmi ${CGW_IMG_ID}:${CGW_IMG_TAG} >/dev/null 2>&1 || true
	@docker rmi ${CGW_BUILD_ENV_IMG_ID}:${CGW_BUILD_ENV_IMG_TAG} >/dev/null 2>&1 || true
	@echo Done!

run: stop cgw-img run_docker_services
	@./run_cgw.sh "${CGW_IMG_ID}:${CGW_IMG_TAG}" ${CGW_IMG_CONTAINER_NAME}

start-multi-cgw: cgw-img
	@cd ./utils/docker && python3 StartMultiCGW.py --start && cd -

stop-multi-cgw:
	@cd ./utils/docker && python3 StartMultiCGW.py --stop && cd -

run_docker_services:
	@cd ./utils/docker/ && docker compose up -d

run-tests:
	@cd ./tests && ./run.sh

# Proxy-specific variables
PROXY_IMG_ID := openlan-proxy-cgw-img
PROXY_IMG_TAG := $(shell \
	if [[ `git status --porcelain --untracked-files=no` ]]; then \
		echo "`git rev-parse --short HEAD`-proxy-dirty"; \
	else \
		echo "`git rev-parse --short HEAD`-proxy"; \
	fi)
PROXY_IMG_CONTAINER_NAME := "openlan_proxy"

.PHONY: proxy-img stop-proxy run-proxy

# Builds proxy image using the same build environment
proxy-img: cgw-build-env-img
	@docker build --file Dockerfile.proxy \
		--build-arg="CGW_CONTAINER_BUILD_REV=${PROXY_IMG_TAG}" \
		--tag ${PROXY_IMG_ID}:${PROXY_IMG_TAG} \
		.
	@echo Proxy Docker build done;

stop-proxy:
	@echo "Stopping / removing container ${PROXY_IMG_CONTAINER_NAME}"
	@docker stop ${PROXY_IMG_CONTAINER_NAME} > /dev/null 2>&1 || true;
	@docker container rm ${PROXY_IMG_CONTAINER_NAME} > /dev/null 2>&1 || true;

run-proxy: stop-proxy proxy-img
	@./run_proxy.sh "${PROXY_IMG_ID}:${PROXY_IMG_TAG}" ${PROXY_IMG_CONTAINER_NAME}

clean-proxy: stop-proxy
	@echo Cleaning proxy artifacts...
	@docker rmi ${PROXY_IMG_ID}:${PROXY_IMG_TAG} >/dev/null 2>&1 || true
	@echo Done!

# Extends the existing clean target to also clean proxy artifacts
clean-all: clean clean-proxy