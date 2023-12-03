all:


create:
	DTPS_BASE_SELF=create:http://localhost:8001 \
	    python3 filtering_filter_ergo.py


switchbord:
	cargo run --bin dtps-http-rs-server -- --tcp-port 8000

use:
	DTPS_BASE_SELF=http://localhost:8000/ \
	    python3 filtering_filter_ergo.py
