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
COPY Cargo.toml Cargo.lock ./

COPY metrics/Cargo.toml metrics/
COPY perf/Cargo.toml perf/
COPY net-utils/Cargo.toml net-utils/
COPY rpc/Cargo.toml rpc/
COPY rpc-core/Cargo.toml rpc-core/
COPY storage-adapter/Cargo.toml storage-adapter/
COPY storage-hbase/Cargo.toml storage-hbase/
COPY storage-bigtable/Cargo.toml storage-bigtable/
COPY storage-bigtable/build-proto/Cargo.toml storage-bigtable/build-proto/
COPY storage-proto/Cargo.toml storage-proto/
COPY launcher/Cargo.toml launcher/

RUN mkdir -p src metrics/src perf/src net-utils/src rpc/src rpc-core/src storage-adapter/src \
    storage-hbase/src storage-bigtable/src storage-bigtable/build-proto/src storage-proto/src launcher/src && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "pub fn dummy() {}" > metrics/src/lib.rs && \
    echo "pub fn dummy() {}" > perf/src/lib.rs && \
    echo "pub fn dummy() {}" > net-utils/src/lib.rs && \
    echo "pub fn dummy() {}" > rpc/src/lib.rs && \
    echo "pub fn dummy() {}" > rpc-core/src/lib.rs && \
    echo "pub fn dummy() {}" > storage-adapter/src/lib.rs && \
    echo "pub fn dummy() {}" > storage-hbase/src/lib.rs && \
    echo "pub fn dummy() {}" > storage-bigtable/src/lib.rs && \
    echo "pub fn dummy() {}" > storage-bigtable/build-proto/src/lib.rs && \
    echo "pub fn dummy() {}" > storage-proto/src/lib.rs && \
    echo "pub fn dummy() {}" > launcher/src/lib.rs && \
    mkdir -p launcher/src/bin && echo "fn main() {}" > launcher/src/bin/archival-rpc.rs

# Build dependencies only
RUN cargo build --release
RUN rm -rf src

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
