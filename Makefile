.PHONY: build test test-processing

build:
	go build -o gisquick ./cmd/...

test:
	go test ./...

test-processing:
	go test -v ./internal/processing/...
