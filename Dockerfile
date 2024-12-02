
FROM debian:buster-slim

RUN mkdir -p /solana
WORKDIR /solana

ARG AMD64_BINARY
ARG ARM64_BINARY
ARG TARGETARCH

# Set target architecture and copy the appropriate binary
RUN if [ "$TARGETARCH" = "amd64" ]; then \
      cp "$AMD64_BINARY" /solana/archival-rpc; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
      cp "$ARM64_BINARY" /solana/archival-rpc; \
    else \
      echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi

# Expose the necessary port
EXPOSE 8899

ENV RUST_LOG=info

CMD ["./archival-rpc", "--bind-address=0.0.0.0", "--enable-rpc-hbase-ledger-storage", "--rpc-hbase-address=hbase:9090"]
