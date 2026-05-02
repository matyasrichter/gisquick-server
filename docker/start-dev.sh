#!/bin/sh

go build -ldflags="-s -w" -o gisquick cmd/main.go
exec ./gisquick serve
