SHELL=/bin/bash

help:
	@echo '-- Make targets --------------------'
	@echo '[ Settings defined in ./.config.sh ]'
	@echo
	@echo ' plaintext       Extract plaintext version of NYT portion of Gigaword'

plaintext:
	@set -e; \
	source .config.sh; \
	$${PY} -m plaintext \
		--output=$${DATA}/nyt_plaintext \
		$${GIGAWORD}/nyt/nyt*.gz
