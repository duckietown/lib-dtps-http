# syntax=docker/dockerfile:1.4

FROM rust as builder
ENV USER root
WORKDIR /wd
COPY rust rust
COPY Cargo.toml .
# RUN --mount=type=cache,target=/usr/local/cargo/registry \
# --release
RUN cargo build  --target-dir /wd/target
RUN find /wd/target -type f
RUN cp /wd/target/debug/dtps-http-rust-clock /tmp/app

# get cloudflare executable

FROM curlimages/curl as builder2
ARG TARGETARCH
ENV USER root
# important: -L follows redirects
RUN curl -L -o /tmp/cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-$TARGETARCH \
 && ls /tmp -l -a \
 && chmod +x /tmp/cloudflared

FROM ubuntu:22.04
WORKDIR /wd

COPY --from=builder2 /tmp/cloudflared /
COPY --from=builder /tmp/app /

RUN ls -a -l /
RUN arch
RUN ["/app", "--version"]
RUN ["/cloudflared", "--version"]
ENV RUST_BACKTRACE=full
ENV RUST_LOG=debug
ENTRYPOINT ["/app", "--cloudflare-executable=/cloudflared"]
