#!/bin/bash
set -e

VERSION="1.9.1"
PROTECTED_MODE="no"

# Hardcode some values to the core package
LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.Version=${VERSION}"
if [ -d ".git" ]; then
	LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.GitSHA=$(git rev-parse --short HEAD)"
fi
LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.BuildTime=$(date +%FT%T%z)"
if [ "$PROTECTED_MODE" == "no" ]; then
	LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.ProtectedMode=no"
fi

if [ "$1" == "update-version" ]; then
	# update the versions in the README.md and Dockerfile
	sed -i '' "s/version-.\..\../version-$VERSION/g" README.md
	sed -i '' "s/ENV\ TILE38_VERSION\ .\..\../ENV TILE38_VERSION $VERSION/g" docker/Dockerfile
	exit
fi

# Check go install
if [ "$(which go)" == "" ]; then
	echo "error: Go is not installed. Please download and follow installation instructions at https://golang.org/dl to continue."
	exit 1
fi

# Check go version
GOVERS="$(go version | cut -d " " -f 3)"
if [ "$GOVERS" != "devel" ]; then
	vercomp () {
		if [[ $1 == $2 ]]
		then
			echo "0"
			return
		fi
		local IFS=.
		local i ver1=($1) ver2=($2)
		# fill empty fields in ver1 with zeros
		for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
		do
			ver1[i]=0
		done
		for ((i=0; i<${#ver1[@]}; i++))
		do
			if [[ -z ${ver2[i]} ]]
			then
				# fill empty fields in ver2 with zeros
				ver2[i]=0
			fi
			if ((10#${ver1[i]} > 10#${ver2[i]}))
			then
				echo "1"
				return
			fi
			if ((10#${ver1[i]} < 10#${ver2[i]}))
			then
				echo "-1"
				return
			fi
		done
		echo "0"
		return
	}
	GOVERS="${GOVERS:2}"
	EQRES=$(vercomp "$GOVERS" "1.7")  
	if [ "$EQRES" == "-1" ]; then
		  echo "error: Go '1.7' or greater is required and '$GOVERS' is currently installed. Please upgrade Go at https://golang.org/dl to continue."	
		  exit 1
	fi
fi

export GO15VENDOREXPERIMENT=1

cd $(dirname "${BASH_SOURCE[0]}")
OD="$(pwd)"

package(){
	echo Packaging $1 Binary
	bdir=tile38-${VERSION}-$2-$3
	rm -rf packages/$bdir && mkdir -p packages/$bdir
	GOOS=$2 GOARCH=$3 ./build.sh
	if [ "$2" == "windows" ]; then
		mv tile38-server packages/$bdir/tile38-server.exe
		mv tile38-cli packages/$bdir/tile38-cli.exe
		mv tile38-benchmark packages/$bdir/tile38-benchmark.exe
	else
		mv tile38-server packages/$bdir
		mv tile38-cli packages/$bdir
		mv tile38-benchmark packages/$bdir
	fi
	cp README.md packages/$bdir
	cp CHANGELOG.md packages/$bdir
	cp LICENSE packages/$bdir
	cd packages
	if [ "$2" == "linux" ]; then
		tar -zcf $bdir.tar.gz $bdir
	else
		zip -r -q $bdir.zip $bdir
	fi
	rm -rf $bdir
	cd ..
}

if [ "$1" == "package" ]; then
	rm -rf packages/
	package "Windows" "windows" "amd64"
	package "Mac" "darwin" "amd64"
	package "Linux" "linux" "amd64"
	package "FreeBSD" "freebsd" "amd64"
	exit
fi

if [ "$1" == "vendor" ]; then
	pkg="$2"	
	if [ "$pkg" == "" ]; then
		echo "no package specified"
		exit
	fi
	if [ ! -d "$GOPATH/src/$pkg" ]; then
		echo "invalid package"
		exit
	fi
	rm -rf vendor/$pkg/
	mkdir -p vendor/$pkg/
	cp -rf $GOPATH/src/$pkg/* vendor/$pkg/
	rm -rf vendor/$pkg/.git
	exit
fi

# temp directory for storing isolated environment.
TMP="$(mktemp -d -t tile38.XXXX)"
function rmtemp {
	rm -rf "$TMP"
}
trap rmtemp EXIT

if [ "$NOCOPY" != "1" ]; then
	# copy all files to an isloated directory.
	WD="$TMP/src/github.com/tidwall/tile38"
	export GOPATH="$TMP"
	for file in `find . -type f`; do
		# TODO: use .gitignore to ignore, or possibly just use git to determine the file list.
		if [[ "$file" != "." && "$file" != ./.git* && "$file" != ./data* && "$file" != ./tile38-* ]]; then
			mkdir -p "$WD/$(dirname "${file}")"
			cp -P "$file" "$WD/$(dirname "${file}")"
		fi
	done
	cd $WD
fi

# build and store objects into original directory.
go build -ldflags "$LDFLAGS" -o "$OD/tile38-server" cmd/tile38-server/*.go
go build -ldflags "$LDFLAGS" -o "$OD/tile38-cli" cmd/tile38-cli/*.go
go build -ldflags "$LDFLAGS" -o "$OD/tile38-benchmark" cmd/tile38-benchmark/*.go
go build -ldflags "$LDFLAGS" -o "$OD/tile38-luamemtest" cmd/tile38-luamemtest/*.go

# test if requested
if [ "$1" == "test" ]; then
	$OD/tile38-server -p 9876 -d "$TMP" -q &
	PID=$!
	function testend {
		kill $PID &
	}
	trap testend EXIT
	cd tests && go test && cd ..
	go test $(go list ./... | grep -v /vendor/ | grep -v /tests)
fi

# cover if requested
if [ "$1" == "cover" ]; then
	$OD/tile38-server -p 9876 -d "$TMP" -q &
	PID=$!
	function testend {
		kill $PID &
	}
	trap testend EXIT
	go test -cover $(go list ./... | grep -v /vendor/)
fi


