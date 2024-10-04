FROM rust:1.64 as build

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    libclang-dev \
    libudev-dev

RUN USER=root cargo new --bin solana
WORKDIR /solana

COPY . /solana

RUN cargo build --release



FROM rust:1.64

RUN mkdir -p /solana

WORKDIR /solana

COPY --from=build /solana/target/release/archival-rpc .

EXPOSE 8899

CMD ["./archival-rpc", "--bind-address=0.0.0.0", "--enable-rpc-hbase-ledger-storage", "--rpc-hbase-address=hbase:9090"]