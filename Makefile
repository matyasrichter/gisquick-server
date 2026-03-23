.PHONY: build test

build:
	go build -o gisquick ./cmd/main.go

test:
	go test ./...
