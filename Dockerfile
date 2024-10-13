# First stage: Build the project
FROM rust:1.70.0 as build

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    libclang-dev \
    libudev-dev

# Create a new empty shell project
RUN USER=root cargo new --bin solana
WORKDIR /solana

# Copy only the Cargo.toml and Cargo.lock to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Build the dependencies and cache them
RUN cargo build --release
RUN rm -rf src

# Now copy the full source code
COPY . /solana

# Build the final binary
RUN cargo build --release

# Second stage: Create a minimal runtime image
FROM debian:buster-slim

# Create the directory for the binary
RUN mkdir -p /solana

WORKDIR /solana

# Copy the compiled binary from the builder stage
COPY --from=build /solana/target/release/archival-rpc .

# Expose the necessary port
EXPOSE 8899

# Define the default command
CMD ["./archival-rpc", "--bind-address=0.0.0.0", "--enable-rpc-hbase-ledger-storage", "--rpc-hbase-address=hbase:9090"]
