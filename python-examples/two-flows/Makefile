

publisher:
	python3 twoflows_publisher.py --tcp-port 14582 --topic node/out --unix-path /tmp/twoflows_publisher

subscriber:
	python3 twoflows_subscriber.py --url "http+unix://%2ftmp%2ftwoflows_publisher/node/out/"
