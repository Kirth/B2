# B2 — scripting runtime
# Usage: make [target]; run script with: make run SCRIPT=hello.s

.PHONY: build test run repl install check fmt clean help

# Default: build
all: build

build:
	cargo build

# Release build
release:
	cargo build --release

test:
	cargo test

# Run a script (e.g. make run SCRIPT=hello.s)
run:
	@if [ -z "$(SCRIPT)" ]; then \
		echo "Usage: make run SCRIPT=path/to/script.s"; \
		exit 1; \
	fi
	cargo run -- $(SCRIPT)

# Run REPL
repl:
	cargo run -- repl

# Install binary to cargo bin dir
install:
	cargo install --path .

# Lint and format check (no write)
check:
	cargo clippy -- -D warnings
	cargo fmt -- --check

# Format code
fmt:
	cargo fmt

# Remove build artifacts
clean:
	cargo clean

help:
	@echo "B2 Makefile targets:"
	@echo "  make build     - build debug binary (default)"
	@echo "  make release  - build release binary"
	@echo "  make test     - run tests"
	@echo "  make run SCRIPT=file.s - run a B2 script"
	@echo "  make repl     - start interactive REPL"
	@echo "  make install  - install binary (cargo install --path .)"
	@echo "  make check    - clippy + fmt check"
	@echo "  make fmt      - format code"
	@echo "  make clean   - remove build artifacts"
	@echo "  make help    - show this help"
