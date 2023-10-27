

publisher:
	python3 image_publisher.py --tcp-port 8324

proxy:
	cargo run --bin dtps-http-rs-server -- --tcp-port 8325 --proxy node=http://localhost:8324/
