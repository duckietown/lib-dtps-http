# syntax=docker/dockerfile:1.4

FROM rust as builder1
ENV USER root
WORKDIR /wd
COPY rust rust
COPY rustfmt.toml .
COPY Cargo.toml .
COPY static static
RUN find .

ARG CARGO_PROFILE=release
ARG DEST=release

RUN cargo build --profile $CARGO_PROFILE --target-dir /wd/target
RUN rm -rf /wd/target/$DEST/deps/
RUN rm -rf /wd/target/$DEST/build/
RUN rm -rf /wd/target/$DEST/examples/
RUN rm -rf /wd/target/$DEST/incremental/
RUN rm -rf /wd/target/$DEST/bin/dtps-rust
RUN rm -rf /wd/target/$DEST/bin/dtps-http-rs-server-example-clock
RUN rm -rf /wd/target/$DEST/bin/dtps-http-rs-server-stress-test
RUN rm -rf /wd/target/$DEST/bin/dtps-http-rs-subscribe
RUN rm -rf /wd/target/$DEST/bin/dtps-http-rs-client-stats
RUN rm -rf /wd/target/$DEST/bin/dtps-http-rs-listen
 
# get cloudflare executable

FROM alpine/curl as builder2
ARG TARGETARCH
# important: -L follows redirects
RUN curl -L -o /tmp/cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-$TARGETARCH
RUN chown root:root /tmp/cloudflared
RUN chmod +x /tmp/cloudflared

FROM ubuntu:22.04
# XXX: somehow it doesn't work with alpine
# FROM alpine
WORKDIR /wd

RUN apt-get update && apt-get install -y ca-certificates # needed for https to work

COPY --from=builder2 /tmp/cloudflared /usr/bin

ARG DEST=release
COPY --from=builder1 /wd/target/$DEST/dtps-http-rs-server /usr/bin/dtps-http-rs-server
RUN ls -a -l -h -S /usr/bin

ENV RUST_LOG="warn,dtps_http=debug"
ENV RUST_BACKTRACE=full
RUN <<EOF
    /usr/bin/cloudflared --version
    /usr/bin/dtps-http-rs-server --version
EOF

ENTRYPOINT ["/usr/bin/dtps-http-rs-server"]
