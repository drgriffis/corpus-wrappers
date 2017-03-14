SHELL=/bin/bash

help:
	@echo '-- Make targets -------------------'
	@echo '[ Settings defined in ./.config.sh ]'
	@echo
	@echo '  plaintext     generate plaintext of Wikipedia articles'

plaintext:
	@set -e; \
	source .config.sh; \
	if [ ! -e $${TMPFILE} ]; then \
		$${PY} -m extract_articles $${INFILE} $${TMPFILE}; \
	fi; \
	$${PRL} wikifil.pl < $${TMPFILE} > $${OUTFILE}; \
	rm -f $${TMPFILE}
