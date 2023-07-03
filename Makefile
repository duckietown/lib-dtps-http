all:

.PHONY: build publish bump bump-patch bump-minor bump-major

build:
	rm -rf dist/*
	poetry build -f wheel

publish: build
	twine upload dist/*whl

bump: bump-patch

bump-patch:
	./bump-version.sh patch
	git push --tags

bump-minor:
	./bump-version.sh minor
	git push --tags

bump-major:
	./bump-version.sh major
	git push --tags

.PHONY: docs docs-serve

docs:
	mkdocs build

docs-serve:
	mkdocs serve
