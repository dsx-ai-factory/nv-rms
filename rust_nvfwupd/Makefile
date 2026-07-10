# Makefile for nvfwupd Rust build
#
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.
#

CARGO := cargo
BINARY_NAME := nvfwupd
# In the RMS workspace, Cargo places artifacts at the workspace root.
TARGET_DIR := ../target
RELEASE_DIR := $(TARGET_DIR)/release
DEBUG_DIR := $(TARGET_DIR)/debug

.PHONY: all build release debug clean distclean test lint fmt check help install

# Default target: release build
all: release

# Release build (optimized)
release:
	$(CARGO) build --release
	@echo "Release binary built at: $(RELEASE_DIR)/$(BINARY_NAME)"

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
