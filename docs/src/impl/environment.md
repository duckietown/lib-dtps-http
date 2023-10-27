# Environment variables


## General

### `DTPS_HTTP_MASK_ORIGIN`

If set, the proxy components will mask the real origin (not showing the URL in `Content-Location`).
In this way, the client will not be able to connect directly to the origin.

This is useful for debug purposes, and for particular setups with NATs where it is known that the client cannot connect directly to the origin.


## Python-specific environment variables
