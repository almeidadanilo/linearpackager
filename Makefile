BINARY    := linear-packager
CONFIG    ?= config.json
VERSION   := $(shell git describe --tags --always 2>/dev/null || echo "1.0.0")
LDFLAGS   := -ldflags "-X main.Version=$(VERSION2)"
INSTALL_BIN  := /usr/local/bin
INSTALL_SVC  := /etc/systemd/system
CHANNELS_DIR := /opt/linear-packager/channels

.PHONY: build run clean deps lint install uninstall new-channel

# ── Local development ────────────────────────────────────────────────────────

build:
	go build $(LDFLAGS) -o $(BINARY) ./cmd/packager

run: build
	./$(BINARY) -config $(CONFIG)

clean:
	rm -f $(BINARY)
	rm -rf output/

deps:
	go mod tidy

lint:
	go vet ./...

# ── EC2 / server deployment ───────────────────────────────────────────────────

## install: copy binary + systemd unit to the server
install: build
	install -d $(INSTALL_BIN)
	install -m 755 $(BINARY) $(INSTALL_BIN)/$(BINARY)
	install -d $(INSTALL_SVC)
	install -m 644 deploy/linear-packager@.service $(INSTALL_SVC)/linear-packager@.service
	systemctl daemon-reload
	@echo "Binary installed to $(INSTALL_BIN)/$(BINARY)"
	@echo "Service unit installed. Start with: systemctl start linear-packager@<channel-id>"

## uninstall: stop all channels and remove installed files
uninstall:
	-systemctl stop 'linear-packager@*'
	-systemctl disable 'linear-packager@*'
	rm -f $(INSTALL_SVC)/linear-packager@.service
	rm -f $(INSTALL_BIN)/$(BINARY)
	systemctl daemon-reload
	@echo "Uninstall complete."

## new-channel: scaffold a channel directory (usage: make new-channel CHANNEL=ch1)
new-channel:
	@test -n "$(CHANNEL)" || (echo "Usage: make new-channel CHANNEL=<id>"; exit 1)
	install -d $(CHANNELS_DIR)/$(CHANNEL)/output
	@if [ ! -f $(CHANNELS_DIR)/$(CHANNEL)/config.json ]; then \
		cp config.json $(CHANNELS_DIR)/$(CHANNEL)/config.json; \
		echo "Created $(CHANNELS_DIR)/$(CHANNEL)/config.json — edit it before starting."; \
	else \
		echo "$(CHANNELS_DIR)/$(CHANNEL)/config.json already exists, skipping copy."; \
	fi
	@echo "Channel directory ready: $(CHANNELS_DIR)/$(CHANNEL)"
	@echo "Enable + start: systemctl enable --now linear-packager@$(CHANNEL)"
