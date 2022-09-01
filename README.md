# dbt_datawaves


## What does this dbt package do?


This dbt package creates models on top of [Datawaves](https://datawaves.xyz/) [Decoded Projects Data](https://docs.datawaves.xyz/evm-blockchains/decoded-projects-data) that:

* Translate smart contract function calls and events into domain models
* Enrich trades and mints with details about associated the such as USD price for a token
* Provide balance table at different time granularity


## Abstractions


The pre-built abstractions data models can be divided into two categories. The project abstractions are project-level models built on top of the Decoded Projects Data in Datawaves. And the sector abstractions combine models from multiple projects into one table representing a domain, e.g., NFT.


### Project Abstractions (Views)

| Project | Models | description | Supported Chains |
|---|---|---|---|
| ens | [ens_ethereum_registrations](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/ens/ens_ethereum_registrations.sql) | Each record represents a registration on ENS | Ethereum |
| ens | [ens_ethereum_reverse_registrars](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/ens/ens_ethereum_reverse_registrars.sql) | Each record represents a reverse registrar on ENS | Ethereum |
| opensea | [opensea_trades](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/opensea/opensea_trades.sql) | Each record represents a trade in OpenSeas, enriched with USD price of the trade. | Ethereum |
| cryptopunks | [cryptopunks_trades](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/cryptopunks/cryptopunks_trades.sql) | Each record represents a trade in CryptoPunksMarket, enriched with USD price of the trade. | Ethereum |


### Sector Abstractions (Views)


| Sector | Models | description | Supported Chains |
|---|---|---|---|
| nft | [nft_trades](https://github.com/datawaves-xyz/dbt_datawaves/blob/master/models/nft/nft_trades.sql) | Each record represents a trade, enriched with USD price of the trade. | Ethereum |
| nft | [nft_mints](https://github.com/datawaves-xyz/dbt_datawaves/blob/master/models/nft/nft_mints.sql) | Each record represents an ERC721/ERC1155 token that has been minted. | Ethereum |
| erc20 | [erc20_ethereum_transfers](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/erc1155/erc20_ethereum_transfers.sql) | Each record represents an ERC20 token transfer event. | Ethereum |
| erc721 | [erc721_ethereum_transfers](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/erc1155/erc721_ethereum_transfers.sql) | Each record represents an ERC721 token transfer event. | Ethereum |
| erc1155 | [erc1155_ethereum_transfers](https://github.com/datawaves_xyz/dbt_ethereum/blob/master/models/erc1155/erc1155_ethereum_transfers.sql) | Each record represents an ERC1155 token transfer event. | Ethereum |


## How do I use this dbt package?

### Prerequisites

To use this dbt project, you must have the following:

* Use Datawaves ETL Cloud to sync data into your destination.
* Make sure every source defined in `*_source.yml` exists in your destination. You can run `dbt source freshness` command to ensure they are "fresh".

![](./assets/dbt_datawaves_architecture.png)


### Install the package

Include in your packages.yml:


```yml
packages:
  - git: "https://github.com/datawaves-xyz/dbt_datawaves"
    revision: "0.0.1"
```


## Contribute

Additional contributions to this package are very welcome! Please create issues or open PRs against main.


## Database support


This package has been tested on Databricks.

