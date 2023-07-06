all:

tag=andreacensi/dtps-rust-demo

build:
	docker buildx build --progress plain --platform linux/arm64,linux/amd64 --push --tag $(tag) .


creds=$(realpath $(PWD)/..)
run-demo:
	docker pull  $(tag)
	docker run --init  -it -v $(creds):/creds:ro $(tag)  --tunnel /creds/test-dtps1-tunnel.json
