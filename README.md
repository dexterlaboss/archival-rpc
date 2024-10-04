## Solana Archival RPC Service

Fast and resource-efficient Solana RPC service that does not require as many resources as a full Solana validator node. Focusing on data retrieval endpoints and currently supporting all archival-related RPC calls.

Archival RPC also introduces alternative storage with a self-hosted HBase database, which is 10x+ cheaper than BigTable and you can run it on-prem to achieve sub 10ms RPC calls to methods like getSignaturesForAddress. It reuses Solana Labs Rust validator code without any data structure changes, which means that you can just import BigTable backup to HBase and you are ready to go.

## Overview

The Archival RPC is structured into two primary components for enhanced scalability and separation of concerns:

- **RPC Server**: This is the repository you're currently viewing. It serves as the backbone for communication.
- **Ingestor Module**: Located at [ingestor-kafka](https://github.com/dexterlaboss/ingestor-kafka), this component is dedicated to data ingestion. 
By segregating data ingestion from the serving layer, we provide a scalable architecture that allows each component to scale independently based on demand. The ingestor module is equipped to pull full, unparsed blocks directly from a Kafka topic, with ongoing efforts to integrate GRPC support for enhanced data interchange.


![How we run it!](https://dexterlab.com/content/images/2024/02/Screenshot-2024-02-28-at-11.12.42-2.png "How we run it")


## Quick Setup Overview

In the near future, we're introducing several resources to simplify the setup and maintenance process, including prebuild releases, Docker images, and Helm charts (with HBase support). Below is a brief guide for those eager to get started. Detailed documentation will follow.

**Starting with Archival RPC**

- **Clone and compile Archival RPC**: This is your first step towards setting up.
- **Connecting to BigTable**: If you already have a BigTable instance, you can simply connect to it to offload expensive validator resources.

**Full On-Prem Setup**

For a comprehensive on-premise setup, additional steps are required:
- **HBase and Kafka**: Ensure you have an operational HBase cluster and a Kafka instance.
- **Writing to Kafka**: Utilize Greyser plugins or scripts to push raw blocks to your Kafka topic.
- **Data Ingestion**: Clone and compile the dexterlaboss/ingestor-kafka.
- **Configuration**: Provide the storage-ingestor with HBase credentials and specify the Kafka topic to stream full blocks. 
Stay tuned for more detailed guides on each step of the process.

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
| `getBlockTime`       |
| `getBlocks`       |
| `getBlocksWithLimit`       |

