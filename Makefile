.PHONY: format perms path

format:
	@command -v shfmt >/dev/null 2>&1 || { echo "ERROR: shfmt not found. Install it (e.g. 'brew install shfmt')." >&2; exit 1; }
	shfmt -i 4 -sr -w bin/local-data bin/hive-b
	find lib/local_data -type f -name '*.sh' -print0 | xargs -0 shfmt -i 4 -sr -w

perms:
	chmod +x bin/local-data bin/hive-b

path:
	@echo 'Add this to your shell profile (e.g. ~/.zshrc):'
	@echo '  export PATH="$(CURDIR)/bin:$$PATH"'
