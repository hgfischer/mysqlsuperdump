#!/usr/bin/make -f

export CGO_ENABLED=0
export GO111MODULE=on

REGISTRY=skpr/mysqlsuperdump
VERSION=$(shell git describe --tags --always)

# Run all lint checking with exit codes for CI.
lint:
	golint -set_exit_status `go list ./... | grep -v /vendor/`

# Run go fmt against code
fmt:
	go fmt ./...

vet:
	go vet ./...

# Run tests with coverage reporting.
test:
	gotestsum -- -coverprofile=cover.out ./...

.PHONY: *