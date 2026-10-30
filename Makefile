FUZZTIME ?= 30s

.PHONY: test fuzz test-full

all: main.go
	go build -o atlas main.go

test:
	go test ./... -v

fuzz:
	go test ./pkg/wal -run='^$$' -fuzz='^FuzzDeserializeRecord$$' -fuzztime=$(FUZZTIME)
	go test ./pkg/wal -run='^$$' -fuzz='^FuzzDeserializeEntry$$' -fuzztime=$(FUZZTIME)

test-full: test fuzz
