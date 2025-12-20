.PHONY: format perms path

format:
	shfmt -i 4 -sr -w bin/local-data lib/local_data/**/*.sh

perms:
	chmod +x bin/local-data bin/hive-b

path:
	@echo 'Add this to your shell profile (e.g. ~/.zshrc):'
	@echo '  export PATH="$(CURDIR)/bin:$$PATH"'
