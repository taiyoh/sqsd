GIT_VERSION:=$(shell git describe --tags)
CURRENT_REVISION=$(shell git rev-parse --short HEAD)
CURRENT_DATE=$(shell date +"%FT%T%Z")
LDFLAGS="-s -w -X sqsd.version=$(GIT_VERSION) -X main.commit=$(CURRENT_REVISION) -X main.date=$(CURRENT_DATE)"

.PHONY: get-deps test install

install:
	cd cmd/sqsd && go install

test:
	go test -v -race -timeout 30s
	go vet

get-deps:
	go get -u github.com/golang/dep/cmd/dep
	dep ensure

build:
	rm -f dist/sqsd_linux_amd64 dist/sqsd_darwin_amd64 dist/sqsd_windows_amd64
	GOOS=linux GOARCH=amd64 go build -o dist/sqsd_linux_amd64 -ldflags=$(LDFLAGS) cmd/sqsd/main.go
	GOOS=darwin GOARCH=amd64 go build -o dist/sqsd_darwin_amd64 -ldflags=$(LDFLAGS) cmd/sqsd/main.go
	GOOS=windows GOARCH=amd64 go build -o dist/sqsd_windows_amd64.exe -ldflags=$(LDFLAGS) cmd/sqsd/main.go

release:
	goreleaser --rm-dist
