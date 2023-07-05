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



demo1-a:
	dtps-server-example-clock --tcp-port 8081 --unix-path /tmp/mine
demo1-b:
	dtps-proxy --tcp-port 8082  --mask-origin  --url http://localhost:8081/
demo1-c:
	dtps-proxy --tcp-port 8083  --mask-origin  --url http://localhost:8082/
demo1-d:
	dtps-proxy --tcp-port 8084  --mask-origin  --url http://localhost:8083/
demo1-e:
	dtps-client-stats --inline-data http://localhost:8084/
