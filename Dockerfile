FROM rust:1.70.0 as build

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    libclang-dev \
    libudev-dev

ARG TARGETARCH
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        echo "x86_64-unknown-linux-gnu" > /target_arch; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        echo "aarch64-unknown-linux-gnu" > /target_arch; \
    else \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi && \
    rustup target add $(cat /target_arch)

# Create a new Rust project (for caching dependencies)
RUN USER=root cargo new --bin solana
WORKDIR /solana

# Copy the full source code
COPY . .

RUN RUST_TARGET=$(cat /target_arch) && \
    cargo build --release --target $RUST_TARGET && \
    cp /solana/target/$RUST_TARGET/release/archival-rpc /solana/archival-rpc


FROM debian:buster-slim

RUN mkdir -p /solana
WORKDIR /solana

# Copy the compiled binary from the builder stage
COPY --from=build /solana/archival-rpc .

# Expose the necessary port
EXPOSE 8899

ENV RUST_LOG=info

CMD ["./archival-rpc", "--bind-address=0.0.0.0", "--enable-rpc-hbase-ledger-storage", "--rpc-hbase-address=hbase:9090"]
