SHELL=/bin/bash

help:
	@echo '-- Make targets -------------------'
	@echo '[ Settings defined in ./.config.sh ]'
	@echo
	@echo '  plaintext     generate plaintext of Wikipedia articles'

_verify_settings:
	@set -e; \
	source .config.sh; \
	function warn { \
		echo "The following settings must be specified in .config.sh"; \
	}; \
	haswarned=false; \
	if [ -z "$${INFILE}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  INFILE   Input Wikipedia XML dump"; \
	fi; \
	if [ -z "$${TMPFILE}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  TMPFILE  Intermediate temporary file for XML articles only"; \
	fi; \
	if [ -z "$${OUTFILE}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  OUTFILE  Output plaintext file"; \
	fi; \
	if [ -z "$${PY}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  PY       Python executable"; \
	fi; \
	if [ -z "$${PRL}" ]; then \
		if [ "$$haswarned" = false ]; then warn; haswarned=true; fi; \
		echo "  PRL      Perl executable"; \
	fi; \
	if [ "$$haswarned" = true ]; then exit 1; fi

plaintext: _verify_settings
	@set -e; \
	source .config.sh; \
	if [ ! -e $${TMPFILE} ]; then \
		$${PY} -m extract_articles $${INFILE} $${TMPFILE}; \
	fi; \
	$${PRL} wikifil.pl < $${TMPFILE} > $${OUTFILE}; \
	rm -f $${TMPFILE}
