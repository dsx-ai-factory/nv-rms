# Makefile for nvfwupd Rust build
#
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CARGO := cargo
BINARY_NAME := nvfwupd
X86_64_MUSL_TARGET := x86_64-unknown-linux-musl
AARCH64_MUSL_TARGET := aarch64-unknown-linux-musl
# In the RMS workspace, Cargo places artifacts at the workspace root.
TARGET_DIR := ../../target
RELEASE_DIR := $(TARGET_DIR)/release
DEBUG_DIR := $(TARGET_DIR)/debug
X86_64_MUSL_RELEASE_DIR := $(TARGET_DIR)/$(X86_64_MUSL_TARGET)/release
AARCH64_MUSL_RELEASE_DIR := $(TARGET_DIR)/$(AARCH64_MUSL_TARGET)/release

.PHONY: all build release nvfwupd-x86 nvfwupd-arm64 debug clean distclean test lint fmt check help install

# Default target: release build
all: release

# Release build (optimized)
release:
	$(CARGO) build --release
	@echo "Release binary built at: $(RELEASE_DIR)/$(BINARY_NAME)"

# Standalone x86_64 Linux CLI build
nvfwupd-x86:
	$(CARGO) build --locked --release -p $(BINARY_NAME) --target $(X86_64_MUSL_TARGET)
	@echo "x86_64 nvfwupd binary built at: $(X86_64_MUSL_RELEASE_DIR)/$(BINARY_NAME)"

# Standalone arm64 Linux CLI build
nvfwupd-arm64:
	$(CARGO) zigbuild --locked --release -p $(BINARY_NAME) --target $(AARCH64_MUSL_TARGET)
	@echo "arm64 nvfwupd binary built at: $(AARCH64_MUSL_RELEASE_DIR)/$(BINARY_NAME)"

# Debug build
debug:
	$(CARGO) build
	@echo "Debug binary built at: $(DEBUG_DIR)/$(BINARY_NAME)"

# Alias for debug build
build: debug

# Run all tests
test:
	$(CARGO) test

# Run clippy linter
lint:
	$(CARGO) clippy -- -D warnings

# Format code
fmt:
	$(CARGO) fmt

# Check formatting without modifying
fmt-check:
	$(CARGO) fmt -- --check

# Type-check without building
check:
	$(CARGO) check

# Clean build artifacts (cargo target directory)
clean:
	$(CARGO) clean

# Remove ALL generated files: target/, Cargo.lock, and any stray binaries
distclean: clean
	rm -rf $(TARGET_DIR) Cargo.lock
	rm -f $(BINARY_NAME)
	@echo "All build artifacts and binaries removed."

# Install binary to ~/.cargo/bin
install: release
	$(CARGO) install --path .

# Build documentation
doc:
	$(CARGO) doc --no-deps

# Run with arguments (usage: make run ARGS="show_version -t ip=10.0.0.1 user=admin password=pass")
run: debug
	$(DEBUG_DIR)/$(BINARY_NAME) $(ARGS)

help:
	@echo "nvfwupd Rust Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all (default)  - Build release binary"
	@echo "  release        - Build optimized release binary"
	@echo "  nvfwupd-x86    - Build standalone x86_64 Linux CLI binary"
	@echo "  nvfwupd-arm64  - Build standalone arm64 Linux CLI binary"
	@echo "  debug / build  - Build debug binary"
	@echo "  test           - Run all tests"
	@echo "  lint           - Run clippy linter"
	@echo "  fmt            - Format source code"
	@echo "  fmt-check      - Check formatting"
	@echo "  check          - Type-check without building"
	@echo "  clean          - Remove build artifacts (target/)"
	@echo "  distclean      - Remove ALL generated files (target/, Cargo.lock, binaries)"
	@echo "  install        - Install binary to ~/.cargo/bin"
	@echo "  doc            - Build documentation"
	@echo "  run ARGS=...   - Build and run with arguments"
	@echo "  help           - Show this help"
