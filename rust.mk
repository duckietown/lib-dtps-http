all:

tag=andreacensi/dtps-rust-demo

RELEASE='--release' # 0.5ms
RELEASE='' # 2.5

run-server-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rust-clock -- --tcp-port 8000'

run-proxy-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rust-clock -- --tcp-port 8001  --proxy node1=http://localhost:8000 --tunnel  test-dtps1-tunnel.json'


run-proxy2-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rust-clock -- --tcp-port 8002  --proxy proxy1=http://localhost:8001 '


run-proxy2-continuous-b:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rust-clock -- --tcp-port 8002  --proxy proxy1=https'


run-proxy3-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rust-clock -- --tcp-port 8003  --proxy proxy2=http://localhost:8002 '

run-client-continuous-to-rust-server:
	cargo watch -c -w rust/src -w rust/bin -E RUST_BACKTRACE=full  -x 'run  $(RELEASE)  --bin dtps-http-rust-client-stats -- --url http://127.0.0.1:8000/ '

run-client-continuous-to-python-server:
	cargo watch -c -w rust/src -w rust/bin -E RUST_BACKTRACE=full  -x 'run  $(RELEASE)  --bin dtps-http-rust-client-stats -- --url http://127.0.0.1:8081/ '


docker-build-debug:
	docker buildx build --build-arg CARGO_PROFILE=dev --build-arg DEST=debug --progress plain --platform linux/arm64,linux/amd64 --push --tag $(tag) .

docker-build-release:
	docker buildx build --build-arg CARGO_PROFILE=release --build-arg DEST=release --progress plain --platform linux/arm64,linux/amd64 --push --tag $(tag) .

creds=$(realpath $(PWD))
run-demo:
	docker pull  $(tag)
	# --init: do not give pid 1 - which makes it hard to kill
	docker run  --init -it -v /tmp/run:/tmp/run -v $(creds):/creds:ro  -p 8000:8000 \
 		$(tag) --tunnel /creds/test-dtps1-tunnel.json --unix-path /tmp/run/demo1

run-demo-nosocket:
	docker pull  $(tag)
	# --init: do not give pid 1 - which makes it hard to kill
	docker run  -d --init  -v /tmp/run:/tmp/run -v $(creds):/creds:ro  -p 8000:8000 \
 		$(tag) --tunnel /creds/test-dtps1-tunnel.json
