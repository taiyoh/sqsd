GIT_VERSION=$(shell git describe --tags | sed 's/^v//')
CURRENT_REVISION=$(shell git rev-parse --short HEAD)
CURRENT_DATE=$(shell date +"%FT%T%z")
LDFLAGS="-s -w -X github.com/taiyoh/sqsd.version=$(GIT_VERSION) -X main.commit=$(CURRENT_REVISION) -X main.date=$(CURRENT_DATE)"

.PHONY: test install build release clean docker

install:
	cd cmd/sqsd && go install

test:
	go test -v -race -timeout 30s
	cd actor && go test -v -race -timeout 30s

PKGDIR=pkg/v$(GIT_VERSION)
DISTDIR=$(PKGDIR)/dist

clean:
	rm -rf $(PKGDIR)/ && mkdir -p $(PKGDIR)/dist

$(PKGDIR)/sqsd_linux_amd64/sqsd:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(@D)/$(@F) -ldflags=$(LDFLAGS) cmd/sqsd/main.go

$(PKGDIR)/sqsd_darwin_amd64/sqsd:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o $(@D)/$(@F) -ldflags=$(LDFLAGS) cmd/sqsd/main.go

$(PKGDIR)/sqsd_windows_amd64/sqsd.exe:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o $(@D)/$(@F) -ldflags=$(LDFLAGS) cmd/sqsd/main.go

$(DISTDIR)/sqsd_$(GIT_VERSION)_linux_amd64.tar.gz: $(PKGDIR)/sqsd_linux_amd64/sqsd
	cd $(PKGDIR)/sqsd_linux_amd64 && tar cvzf $(@F) sqsd && mv $(@F) ../dist

$(DISTDIR)/sqsd_$(GIT_VERSION)_darwin_amd64.zip: $(PKGDIR)/sqsd_darwin_amd64/sqsd
	cd $(PKGDIR)/sqsd_darwin_amd64 && zip $(@F) * && mv $(@F) ../dist

$(DISTDIR)/sqsd_$(GIT_VERSION)_windows_amd64.zip: $(PKGDIR)/sqsd_windows_amd64/sqsd.exe
	cd $(PKGDIR)/sqsd_windows_amd64 && zip $(@F) * && mv $(@F) ../dist

build: clean $(DISTDIR)/sqsd_$(GIT_VERSION)_linux_amd64.tar.gz $(DISTDIR)/sqsd_$(GIT_VERSION)_darwin_amd64.zip $(DISTDIR)/sqsd_$(GIT_VERSION)_windows_amd64.zip

release:
	ghr v$(GIT_VERSION) $(PKGDIR)/dist

docker: $(PKGDIR)/sqsd_linux_amd64/sqsd
	cp $(PKGDIR)/sqsd_linux_amd64/sqsd pkg/sqsd
