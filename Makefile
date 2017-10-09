.PHONY: get-deps test install

install: 
	cd cmd/sqsd && go install

test:
	go test -v -race
	go vet

get-deps:
	go get -u github.com/golang/dep/cmd/dep
	dep ensure

release:
	goreleaser --rm-dist
