FROM rust:1.70.0 as build

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    libclang-dev \
    libudev-dev

# Create a new empty workspace project to cache dependencies
WORKDIR /build

# Copy the full source code
COPY . .

# Build the entire workspace
RUN cargo build --release


FROM debian:buster-slim

RUN mkdir -p /solana

WORKDIR /solana

# Copy the compiled binary from the builder stage
COPY --from=build /build/target/release/archival-rpc .

# Expose the necessary port
EXPOSE 8899

CMD ["./archival-rpc", "--bind-address=0.0.0.0", "--enable-rpc-hbase-ledger-storage", "--rpc-hbase-address=hbase:9090"]
