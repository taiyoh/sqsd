GIT_VERSION=$(shell git describe --tags | sed 's/^v//')
CURRENT_REVISION=$(shell git rev-parse --short HEAD)
CURRENT_DATE=$(shell date +"%FT%T%z")
LDFLAGS="-s -w -X github.com/taiyoh/sqsd.version=$(GIT_VERSION) -X main.commit=$(CURRENT_REVISION) -X main.date=$(CURRENT_DATE)"

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
	rm -rf pkg/v$(GIT_VERSION)/ && mkdir -p pkg/v$(GIT_VERSION)/dist
	GOOS=linux   GOARCH=amd64 go build -o pkg/v$(GIT_VERSION)/sqsd_linux_amd64/sqsd       -ldflags=$(LDFLAGS) cmd/sqsd/main.go
	cd pkg/v$(GIT_VERSION)/sqsd_linux_amd64   && tar cvzf sqsd_$(GIT_VERSION)_linux_amd64.tar.gz sqsd && mv sqsd_$(GIT_VERSION)_linux_amd64.tar.gz ../dist
	GOOS=darwin  GOARCH=amd64 go build -o pkg/v$(GIT_VERSION)/sqsd_darwin_amd64/sqsd      -ldflags=$(LDFLAGS) cmd/sqsd/main.go
	cd pkg/v$(GIT_VERSION)/sqsd_darwin_amd64  && zip sqsd_$(GIT_VERSION)_darwin_amd64.zip * && mv sqsd_$(GIT_VERSION)_darwin_amd64.zip ../dist
	GOOS=windows GOARCH=amd64 go build -o pkg/v$(GIT_VERSION)/sqsd_windows_amd64/sqsd.exe -ldflags=$(LDFLAGS) cmd/sqsd/main.go
	cd pkg/v$(GIT_VERSION)/sqsd_windows_amd64 && zip sqsd_$(GIT_VERSION)_windows_amd64.zip * && mv sqsd_$(GIT_VERSION)_windows_amd64.zip ../dist

release:
	ghr v$(GIT_VERSION) pkg/v$(GIT_VERSION)/dist
