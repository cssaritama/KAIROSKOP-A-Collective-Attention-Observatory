.PHONY: test audit

test:
	pytest tests/

audit: test
