# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# Detect the shared library extension for this platform.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	EXT := dylib
else ifeq ($(OS),Windows_NT)
	EXT := dll
else
	EXT := so
endif

SHARED_LIB := libadbc_driver_spark.$(EXT)
GO_FILES := $(shell git ls-files '*.go' 2>/dev/null | grep -v '/proto/')

.PHONY: help proto build test vet fmt lint sharedlib python-build python-test docs docs-serve examples-go clean

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

proto: ## Lint and regenerate the Spark Connect Go stubs.
	buf lint proto
	buf generate proto

build: ## Build all Go packages.
	go build ./...

test: ## Run the Go unit tests.
	go test ./...

vet: ## Run go vet.
	go vet ./...

fmt: ## Format Go sources (excluding generated stubs).
	gofmt -w $(GO_FILES)

lint: ## Run golangci-lint (must be installed).
	golangci-lint run

sharedlib: ## Build the C-ABI shared library for this platform.
	CGO_ENABLED=1 go build -tags driverlib -buildmode=c-shared -o $(SHARED_LIB) ./c

python-build: ## Build the shared library into the Python package and install it.
	CGO_ENABLED=1 go build -tags driverlib -buildmode=c-shared \
		-o python/adbc_driver_spark/$(SHARED_LIB) ./c
	pip install ./python

python-test: ## Run the Python unit tests.
	pytest python/tests

docs: ## Build the documentation site.
	mkdocs build --strict

docs-serve: ## Serve the documentation site locally.
	mkdocs serve

examples-go: ## Build the Go examples.
	go build ./examples/go/...

clean: ## Remove build artifacts and caches.
	rm -f libadbc_driver_spark.* python/adbc_driver_spark/libadbc_driver_spark.*
	rm -rf site dist python/dist python/build python/*.egg-info
	rm -rf .pytest_cache python/.pytest_cache python/.ruff_cache python/.mypy_cache
	go clean ./... 2>/dev/null || true
