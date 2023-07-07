# syntax=docker/dockerfile:1.4

FROM rust as builder1
ENV USER root
WORKDIR /wd
COPY .git .git
COPY rust rust
COPY Cargo.toml .
# RUN --mount=type=cache,target=/usr/local/cargo/registry \
# --release
RUN cargo build  --target-dir /wd/target
RUN find /wd/target -type f
RUN cp /wd/target/debug/dtps-http-rust-clock /tmp/app
RUN ls -a -l /tmp/app
RUN /tmp/app --version

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

COPY --from=builder2 /tmp/cloudflared /wd/cloudflared
RUN chmod +x /wd/cloudflared
RUN chown root:root /wd/cloudflared
RUN #ls -a -l /wd/cloudflared
COPY --from=builder1 /tmp/app /wd/app
RUN #ls -a -l /wd
RUN chmod +x /wd/app
RUN chown root:root /wd/app
#RUN ls -a -l /wd/app
#RUN ls -a -l /
RUN ./cloudflared --version
RUN ./app --version
ENV RUST_BACKTRACE=full
ENV RUST_LOG=debug
ENTRYPOINT ["/wd/app", "--cloudflare-executable=/cloudflared"]
