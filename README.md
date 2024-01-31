## Solana Lite RPC Service

Fast and resource-efficient Solana RPC service that does not require as many resources as a full Solana validator node. Focusing on data retrieval endpoints and currently supporting all archival-related RPC calls.

Lite RPC also introduces alternative storage with a self-hosted HBase database, which is 10x+ cheaper than BigTable and you can run it on-prem to achieve sub 10ms RPC calls to methods like `getSignaturesForAddress`. It reuses Solana Labs Rust validator code without any data structure changes, which means that you can just import BigTable backup to HBase and you are ready to go.

Data ingestion is separated into another service https://github.com/dexterlaboss/solana-lite-rpc-storage-ingestor so that Lite RPC could have as less dependencies as possible and could scale into thousands of instances.

## Setup

In the upcoming weeks/months, we will add prebuild releases, Docker images, Helm charts (including HBase), and another bit for easier setup and maintenance. For those who are impatient, you can follow the steps:

- clone and compile Lite RPC
- if you have a working BigTable instance you can just stop here and connect to it and start saving traffic from expensive validators.

if you want a full on-prem setup you will need to go extra steps:

- setup HBase cluster
- write raw blocks to Kafka topic, which can be achieved with Greyser plugins
- clone and compile data ingestion service `dexterlaboss/solana-lite-rpc-storage-ingestor`
- provide storage-ingestor HBase credentials and Kafka topic which has full blocks stream

## Startup args

| Environment Variable            | Purpose                         | Default Value                                  |
|---------------------------------|---------------------------------|------------------------------------------------|
| `--bind-address`                | Address for the RPC node        | 0.0.0.0                                   |
| `--rpc-port`                | Port for the RPC service        | 8900                                   |
| `--quiet`                | Quiet mode: suppress normal output        | false                                   |
| `--log`                | Log mode: stream the launcher log        |                                    |
| `--log-path`                | Log file location        |                                    |
| `--log-messages-bytes-limit`                | Maximum number of bytes written to the program log before truncation        |                                    |
| `--enable-rpc-hbase-ledger-storage`                | Fetch historical transaction info from a HBase instance        |                                    |
| `--rpc-hbase-address`                | Address of HBase Thrift instance to use        |                                    |
| `--enable-rpc-bigtable-ledger-storage`                | Fetch historical transaction info from a BigTable instance        |                                    |
| `--rpc-bigtable-instance-name`                | Name of the Bigtable instance to use        |                                    |
| `--rpc-bigtable-app-profile-id`                | Bigtable application profile id to use for requests       |                                    |
| `--rpc-bigtable-timeout`                | Number of seconds before timing out BigTable requests        |                                    |

## Compatibility

| RPC Method                      |
|---------------------------------|
| `getSignaturesForAddress`       |
| `getTransaction`       |
| `getBlock`       |
| `getBlockHeight`       |
| `getBlockTime`       |
| `getBlocks`       |
| `getBlocksWithLimit`       |

