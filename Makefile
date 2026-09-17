BIN        := bin
GOFLAGS    :=
LDFLAGS    := -s -w
AGENT_HOST ?= ec2-user@44.251.166.76
AGENT_KEY  ?= $(HOME)/.ssh/abhi-dev02-c7g.pem

.PHONY: all build orchestrator mock-poold test vet tidy run-poold run demo deploy clean

all: build

build: orchestrator mock-poold

orchestrator:
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/orchestrator ./cmd/orchestrator

mock-poold:
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/mock-poold ./cmd/mock-poold

# Cross-compile the orchestrator for the aarch64 EC2 host.
orchestrator-linux:
	GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(BIN)/orchestrator-linux-arm64 ./cmd/orchestrator

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# Start the mocked pool manager (8 VMs, 10s recycle).
run-poold: mock-poold
	$(BIN)/mock-poold -addr :9090 -size 8

# Run the orchestrator against a local poold (needs run-poold in another shell).
run: orchestrator
	$(BIN)/orchestrator

# Self-contained demo: mock poold + mock dispatch + stub LLM, no external deps.
# Open http://localhost:8080 then: curl -XPOST :8080/webhook -d '{"title":"memory > 40%","tags":["kube_deployment:tachyon"]}'
demo: build
	@echo ">> mock-poold :9090 + orchestrator :8080 (mock dispatch, stub LLM)"
	@DB_PATH=./demo.db WEBHOOK_ADDR=:8080 POOLD_URL=http://localhost:9090 \
	  DISPATCH_MODE=mock LLM_BACKEND=stub WORKERS=4 \
	  sh -c '$(BIN)/mock-poold -addr :9090 -size 8 & POOLD=$$!; \
	         trap "kill $$POOLD" EXIT; sleep 1; $(BIN)/orchestrator'

# Build the linux binary and ship it + the unit to the host.
deploy: orchestrator-linux
	scp -i $(AGENT_KEY) $(BIN)/orchestrator-linux-arm64 $(AGENT_HOST):/tmp/orchestrator
	scp -i $(AGENT_KEY) mcp.json                        $(AGENT_HOST):/tmp/mcp.json
	scp -i $(AGENT_KEY) systemd/orchestrator.service    $(AGENT_HOST):/tmp/orchestrator.service
	ssh -i $(AGENT_KEY) $(AGENT_HOST) ' \
	  sudo install -o agent -g agent -m0755 /tmp/orchestrator /opt/agent/orchestrator; \
	  sudo cp /tmp/mcp.json /opt/agent/mcp.json; \
	  sudo cp /tmp/orchestrator.service /etc/systemd/system/orchestrator.service; \
	  sudo systemctl daemon-reload && sudo systemctl restart orchestrator && \
	  sleep 2 && sudo systemctl is-active orchestrator'

clean:
	rm -rf $(BIN) demo.db smoke.db
