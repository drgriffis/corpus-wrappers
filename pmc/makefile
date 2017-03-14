SHELL=/bin/bash

help:
	@echo '-- Make targets --------------------'
	@echo '[ Settings defined in ./.config.sh ]'
	@echo
	@echo ' plaintext       Extract plaintext version of PMC'

_verify_settings:
	@set -e; \
	source .config.sh; \
	function warn { \
		echo "The following settings must be specified in .config.sh"; \
	}; \
	haswarned=false; \
	if [ -z "$${DATA}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  DATA   Directory containing PMC data files"; \
	fi; \
	if [ -z "$${PY}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  PY     Python executable"; \
	fi; \
	if [ "$$haswarned" = true ]; then exit 1; fi

plaintext: _verify_settings
	@set -e; \
	source .config.sh; \
	$${PY} -m plaintext \
		--output=$${DATA}/plaintext \
		$${DATA}/articles.*.txt.tar.gz
