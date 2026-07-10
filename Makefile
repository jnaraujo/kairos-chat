.PHONY: all test build clean

all: test

test:
	@echo "=================================================="
	@echo "1. Running logical unit tests (pkg/engine)..."
	@echo "=================================================="
	go test -v ./pkg/engine
	@echo ""
	@echo "=================================================="
	@echo "2. Running distributed simulation tests (pkg/integration)..."
	@echo "=================================================="
	go test -v -race ./pkg/integration
	@echo ""
	@echo "=================================================="
	@echo "All tests successfully configured!"
	@echo "=================================================="

build:
	go build -o chat ./cmd/chat

clean:
	rm -f chat
