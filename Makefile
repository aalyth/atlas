.PHONY: all build test vet check clean

all: build

build: main.go
	go build -o atlas main.go

test:
	go test ./...

vet:
	go vet ./...

check: vet test

clean:
	rm -f atlas
