IMAGE ?= guerite
TAG ?= latest
FULL_IMAGE := $(IMAGE):$(TAG)

PYTHON ?= python

.PHONY: help deps lint test build dual-tag tag-ghcr bump-patch push

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

deps: ## Install runtime and dev dependencies
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e '.[dev]'

lint: ## Run ruff lints
	$(PYTHON) -m ruff check .

test: ## Run pytest
	$(PYTHON) -m pytest

build: ## Build Docker image
	docker build -t $(FULL_IMAGE) .

dual-tag: build ## Tag image as ghcr.io/<user>/<image>:<tag>
	docker tag $(FULL_IMAGE) ghcr.io/$(shell whoami)/$(IMAGE):$(TAG)

tag-ghcr: dual-tag ## Convenience alias for dual-tag

bump-patch: ## Bump patch version and create git tag
	@OLD=$$(grep -Po '(?<=^version = ")[^"]+' pyproject.toml); \
	MAJOR=$$(echo $$OLD | cut -d. -f1); \
	MINOR=$$(echo $$OLD | cut -d. -f2); \
	PATCH=$$(echo $$OLD | cut -d. -f3); \
	NEW="$$MAJOR.$$MINOR.$$((PATCH + 1))"; \
	sed -i "s/^version = \"$$OLD\"/version = \"$$NEW\"/" pyproject.toml; \
	git add pyproject.toml; \
	git commit -m "Bump version to $$NEW"; \
	git tag "v$$NEW"; \
	echo "Bumped version: $$OLD -> $$NEW (tagged v$$NEW)"

push: ## Push commits and current tag to origin
	@TAG=$$(git describe --tags --exact-match 2>/dev/null); \
	git push origin main; \
	if [ -n "$$TAG" ]; then \
		echo "Pushing tag $$TAG..."; \
		git push origin "$$TAG"; \
	else \
		echo "No tag on current commit"; \
	fi
