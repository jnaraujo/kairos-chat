.PHONY: all test build clean run

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

# Variáveis configuráveis para execução do chat
ID ?= userA
ADDR ?= localhost:8080
PEERS ?= userB=localhost:8081,userC=localhost:8082

run: build
	./chat -id $(ID) -addr $(ADDR) -peers "$(PEERS)"
