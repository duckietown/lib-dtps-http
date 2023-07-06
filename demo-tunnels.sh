#!/bin/bash

# This array contains the command lines of your programs
commands=(
  "dtps-server-example-clock --tcp-host localhost --tcp-port 8081 --tunnel test-dtps1-tunnel.json"
  "dtps-proxy --tcp-port 8082 --url https://test-dtps1.duckietown.org/"
  "dtps-proxy --tcp-port 8083 --tunnel test-dtps2-tunnel.json --url http://localhost:8084/"
  "dtps-proxy --tcp-port 8084  --url http://localhost:8083/"
  "dtps-client-stats --inline-data http://localhost:8084/"
  )

pids=()

# This trap function will be called when SIGINT is received
trap ctrl_c INT

function ctrl_c() {
    # Kill each job id in the list
    for pid in ${pids[*]}; do
        kill $pid
    done
}

# Start all programs in the background
for cmd in "${commands[@]}"; do
echo "Running $cmd"
    $cmd &
    sleep 10
    pids+=("$!")
done

# Wait for all programs to finish
for pid in ${pids[*]}; do
    wait $pid
done

 
# dtps-server-example-clock --tcp-host localhost --tcp-port 8081 --tunnel test-dtps1-tunnel.json
# dtps-proxy --tcp-port 8082 --url https://test-dtps1.duckietown.org/
# dtps-proxy --tcp-port 8083 --tunnel test-dtps2-tunnel.json --url https://localhost:8084/ 
# dtps-proxy --tcp-port 8084  --url https://localhost:8083/
# dtps-client-stats --inline-data https://localhost:8084/