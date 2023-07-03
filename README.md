# `dtps-http`: HTTP handling for DTPS

This library is part of the DTPS project.

This library handles the transport/proxying of data over HTTP.


## Demos


Run the following commands in different terminals:


This starts a server listening on port 8081 and unix socket `/tmp/mine`:


    dtps-server-example-clock --tcp-port 8081 --unix-path /tmp/mine


These start proxies:

    dtps-proxy --tcp-port 8082 --url http://localhost:8081/
    dtps-proxy --tcp-port 8083 --url http://localhost:8082/
    dtps-proxy --tcp-port 8084 --url http://localhost:8083/

This starts a client reading from the last port:
    
    dtps-client-stats http://localhost:8084/
