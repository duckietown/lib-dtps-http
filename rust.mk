all:

tag=andreacensi/dtps-rust-demo

RELEASE='--release' # 0.5ms
RELEASE='' # 2.5

run-server-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 8000 --unix-path /tmp/demo1-a'

run-proxy1-continuous-http:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 8001  --unix-path /tmp/demo1-b  --proxy node1=http://localhost:8000 --tunnel  test-dtps1-tunnel.json'


run-proxy1-continuous-socket:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 8001  --unix-path /tmp/demo1-b  --proxy node1=unix:///tmp/demo1-a/ --tunnel  test-dtps1-tunnel.json'



run-proxy2-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 8002 --unix-path /tmp/demo1-c  --proxy proxy1=http://localhost:8001 '


run-proxy3-continuous:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 8003  --proxy proxy2=http://localhost:8002 '

run-client-continuous-to-rust-server:
	cargo watch -c -w rust/src -w rust/bin -E RUST_BACKTRACE=full  -x 'run  $(RELEASE)  --bin dtps-http-rs-client-stats -- --url http://127.0.0.1:8000/ '

run-client-continuous-to-python-server:
	cargo watch -c -w rust/src -w rust/bin -E RUST_BACKTRACE=full  -x 'run  $(RELEASE)  --bin dtps-http-rs-client-stats -- --url http://127.0.0.1:8081/ '


run-proxy-external:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 11000 --unix-path /tmp/proxy-external --proxy proxy1=https://www.duckietown.org/ --proxy static2=.'

run-connections-1:
	cd ../dtps-example-agents && cargo watch -c   -x 'run --bin dtps-nodes-all -- AddToInt --unix-path /tmp/demo1/node1/_node --tcp-port 12001'

run-connections-2:
	cd ../dtps-example-agents && cargo watch -c  -x 'run --bin dtps-nodes-all -- AddToInt --unix-path /tmp/demo1/node2/_node --tcp-port 12002'

run-connections-3:
	cargo watch -c -w rust/src   -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 12000 --unix-path /tmp/demo1/_node --proxy node/node1=http+unix://[/tmp/demo1/node1/_node]/node  --proxy node/node2=http+unix://[/tmp/demo1/node2/_node]/node --alias "node/in= node/node1/in" --connect "node/node1/out->node/node2/in" --alias "node/out=node/node2/out" '

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


test-coverage:
	cargo llvm-cov --open


demo-registration-server:
	cargo watch -c -w static -w rust/src -w rust/bin -E RUST_BACKTRACE=full   -x 'run $(RELEASE) --bin dtps-http-rs-server-example-clock -- --tcp-port 9765 --unix-path /tmp/demo1-a'

demo-registration-client:
	dtps-http-py-server-example-clock --tcp-port 8081 --unix-path /tmp/mine \
		--register-switchboard http://localhost:9765/ \
		--register-as node/node1 \
		--register-namespace dtps
