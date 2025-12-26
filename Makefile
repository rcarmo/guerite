IMAGE ?= guerite
TAG ?= latest
FULL_IMAGE := $(IMAGE):$(TAG)

PYTHON ?= python

.PHONY: help lint test build dual-tag tag-ghcr

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nTargets:\n"} /^[a-zA-Z0-9][a-zA-Z0-9_-]*:.*##/ {printf "  %-16s %s\n", $$1, $$2} END {printf "\n"}' $(MAKEFILE_LIST)

lint: ## Run ruff lints
	$(PYTHON) -m ruff check .

test: ## Run pytest
	$(PYTHON) -m pytest

build: ## Build Docker image
	docker build -t $(FULL_IMAGE) .

dual-tag: build ## Tag image as ghcr.io/<user>/<image>:<tag>
	docker tag $(FULL_IMAGE) ghcr.io/$(shell whoami)/$(IMAGE):$(TAG)

tag-ghcr: dual-tag ## Convenience alias for dual-tag




