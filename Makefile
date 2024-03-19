all:

.PHONY: build publish bump bump-patch bump-minor bump-major

build:
	rm -rf dist/*
	poetry build -f wheel

publish: build
	twine upload dist/*whl

bump: bump-patch

bump-patch:
	bumpversion --tag --verbose patch
	git push --tags

bump-minor:
	bumpversion --tag --verbose minor
	git push --tags

bump-major:
	bumpversion --tag --verbose major
	git push --tags

.PHONY: docs docs-serve

docs:
	mkdocs build

docs-serve:
	mkdocs serve



demo1-a:
	dtps-http-py-server-example-clock --tcp-port 8081 --unix-path /tmp/mine --advertise malformed-one --advertise httpde://malformed-two
demo1-b:
	dtps-http-py-proxy --tcp-port 8082  --mask-origin --url http://localhost:8081/
demo1-c:
	dtps-http-py-proxy --tcp-port 8083  --mask-origin --url http://localhost:8082/
demo1-d:
	dtps-http-py-proxy --tcp-port 8084  --mask-origin --url http://localhost:8083/
demo1-e:
	dtps-py-client-stats --inline-data http://localhost:8084/




include rust.mk
