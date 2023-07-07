# syntax=docker/dockerfile:1.4

FROM rust as builder1
ENV USER root
WORKDIR /wd
COPY .git .git
COPY rust rust
COPY Cargo.toml .
# RUN --mount=type=cache,target=/usr/local/cargo/registry \
# --release
ARG CARGO_PROFILE=release
ARG DEST=release

RUN cargo build --profile $CARGO_PROFILE --target-dir /wd/target
RUN find /wd/target -type f
RUN ls -a -l /wd/target/$DEST
RUN cp /wd/target/$DEST/dtps-http-rust-clock /tmp/app
RUN chown root:root /tmp/app
RUN ls -a -l /tmp/app
RUN /tmp/app --version

# get cloudflare executable

FROM alpine/curl as builder2
ARG TARGETARCH
#ENV USER root
# important: -L follows redirects
RUN curl -L -o /tmp/cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-$TARGETARCH
RUN chown root:root /tmp/cloudflared
RUN chmod +x /tmp/cloudflared

FROM ubuntu:22.04
# XXX: somehow it doesn't work with alpine
# FROM alpine
WORKDIR /wd

COPY --from=builder2 /tmp/cloudflared /wd/cloudflared

COPY --from=builder1 /tmp/app /wd/app
#RUN #ls -a -l /wd
#RUN chmod +x /wd/app
#RUN chown root:root /wd/app
##RUN ls -a -l /wd/app
#RUN ls -a -l /

#RUN ./cloudflared --version
#RUN ./app --version
ENV RUST_BACKTRACE=full
ENV RUST_LOG=debug
ENTRYPOINT ["/wd/app", "--cloudflare-executable=/wd/cloudflared"]
