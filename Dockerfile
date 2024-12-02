
FROM debian:buster-slim

RUN mkdir -p /solana
WORKDIR /solana

ARG AMD64_BINARY
ARG ARM64_BINARY
ARG TARGETARCH

COPY output/linux/${TARGETARCH}/archival-rpc /solana/archival-rpc

# Expose the necessary port
EXPOSE 8899

ENV RUST_LOG=info

CMD ["./archival-rpc", "--bind-address=0.0.0.0", "--enable-rpc-hbase-ledger-storage", "--rpc-hbase-address=hbase:9090"]
