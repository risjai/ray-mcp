.PHONY: build test lint vet tidy

# Build all packages.
build:
	go build ./...

# Run the unit test suite.
test:
	go test ./...

# Run the linter via the version pinned as a module tool in go.mod.
lint:
	go tool golangci-lint run ./...

# Run go vet.
vet:
	go vet ./...

# Sync module requirements.
tidy:
	go mod tidy
