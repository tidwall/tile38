#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

if [ "$1" == "" ]; then
	echo "error: missing argument (binary name)"
	exit 1
fi

# Check the Go installation
if [ "$(which go)" == "" ]; then
	echo "error: Go is not installed. Please download and follow installation"\
		 "instructions at https://golang.org/dl to continue."
	exit 1
fi

# Hardcode some values to the core package.
if [ -d ".git" ]; then
	VERSION=$(git describe --tags --abbrev=0)
	GITSHA=$(git rev-parse --short HEAD)
	LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.Version=${VERSION}"
	LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.GitSHA=${GITSHA}"
fi
LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.BuildTime=$(date +%FT%T%z)"

# Generate the core package
core/gen.sh

# Set final Go environment options
LDFLAGS="$LDFLAGS -extldflags '-static'"
export CGO_ENABLED=0

# if [ "$NOMODULES" != "1" ]; then
# 	export GO111MODULE=on
# 	export GOFLAGS=-mod=vendor
# 	go mod vendor
# fi

if [[ "$GORACE" == "1" ]]; then
goflags="$goflags -race"
fi

# Build and store objects into original directory.
go build -ldflags "$LDFLAGS" $goflags -o $1 cmd/$1/*.go
