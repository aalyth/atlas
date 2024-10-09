

all: cmd/atlasd/main.go
	go build -o compctl cmd/compctl/main.go
	sudo mv compctl /usr/local/bin/compctl
	go build -o atlasd cmd/atlasd/main.go

.PHONY: clean
clean:
	rm atlasd
