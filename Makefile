.PHONY: build

build:
	mkdir -p target
	go build -o target ./...
