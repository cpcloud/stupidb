.PHONY: help clean clean-build clean-pyc clean-test clean-mypy clean-direnv \
  	format check test coverage docs
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean-build: ## remove build artifacts
	rm -rf build dist .eggs
	fd --no-ignore '.*\.egg(-info)?' --exec rm -rf

clean-pyc: ## remove Python artifacts
	fd --no-ignore '.*\.py[co]' --exec rm -f
	fd --no-ignore '__pycache__' --exec rm -rf

clean-test: ## remove test and coverage artifacts
	rm -rf .coverage htmlcov .pytest_cache .mypy_cache

clean-mypy:
	rm -rf .mypy_cache

clean-direnv:
	rm -rf .direnv

clean: clean-build clean-pyc clean-test clean-mypy clean-direnv
	@touch .envrc

format:
	isort . --profile=black
	black .
	nixpkgs-fmt .
	prettier --write $$(fd '\.(toml|ya?ml|json)$$' --hidden)

check:
	poetry check
	isort . --profile=black --check
	black --check .
	nix-linter $$(fd --exclude nix/sources.nix '\.nix$$')
	flake8 .
	prettier --check $$(fd '\.(toml|ya?ml|json)$$' --hidden)

test:
	pytest

coverage: ## check code coverage
	coverage run --source stupidb -m pytest
	coverage report -m
	@coverage html
	@echo file://$$(readlink -f htmlcov/index.html)

SPHINX_APIDOC_OPTIONS := members,show-inheritance

docs:  ## generate Sphinx HTML documentation, including API docs
	SPHINX_APIDOC_OPTIONS=${SPHINX_APIDOC_OPTIONS} \
	    sphinx-apidoc --separate --force -o docs/ stupidb stupidb/tests \
	    -H "StupiDB Modules" --ext-doctest --ext-autodoc \
	    --ext-intersphinx --ext-mathjax
	$(MAKE) -C docs clean
	$(MAKE) -C docs doctest
	$(MAKE) -C docs html
