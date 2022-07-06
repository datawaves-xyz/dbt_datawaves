# dbt_datawaves


## What does this dbt package do?


This package models blockchain data from [Datawaves](https://datawaves.xyz/) and provides abstractions like smart contract activity and wallet profile. These abstractions can simplify the process of querying for data.



## Architecture

![](./assets/dbt_datawaves_architecture.png)

## Abstractions

### Event Model

| **model**                                                                                                 | **description**                                                                 |
|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [nft_trades](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/nft/nft_trades.sql) | Each record represents a trade in OpenSea/CryptoPunks, enriched with data about the trade. |
| [nft_mints](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/nft/nft_mints.sql) | Each record represents an ERC721/ERC1155 token that has been minted |


### Labels


#### Whale

| **model** | **description**  |
|-----------|------------------|
| [nft_whale](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/nft_whale.sql) | Top 0.1% in the number of NFT transactions|

#### Smart Money

| **model** | **description**  |
|-----------|------------------|
| [smart_nft_trader](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_trader.sql) | The top 100 addresses in terms of realized profits from NFT sales. |
| [smart_nft_holder](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_holder.sql) | The top 100 addresses in terms of estimated profits of their current NFT portfolio. |
| [smart_nft_sweeper](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_sweeper.sql) | Addresses that have profitably swept at least 5 times at or below the floor price in the last 30 days. |
| [smart_nft_trader](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_trader.sql) | The top 100 addresses in terms of realized profits from NFT sales. |
| [smart_nft_golden_dog_minter](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_golden_dog_minter.sql)| The top 100 addresses that have realized profits on least 2 Golden Dog Collections that were minted in the last 60 days. | 

#### NFT Collector 

| **model** | **description**  |
|-----------|------------------|
| [legendary_nft_trader.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/legendary_nft_trader.sql) | Top 0.1% in the number of NFT transactions. |
| [epic_nft_trader.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/epic_nft_trader.sql) | Top 1% in the number of NFT transactions. |
| [rare_nft_trader.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/rare_nft_trader.sql) | Top 2.5% in the number of NFT transactions. |
| [uncommon_nft_trader.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/uncommon_nft_trader.sql) | Top 10% in the number of NFT transactions. |
| [opensea_nft_trader.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/opensea_nft_trader.sql) | Wallets that have transactions on OpenSea. |
| [blue_chip_nft_holder.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/blue_chip_nft_holder.sql) | Wallets that are currently holding at least one Blue Chip NFT in their portfolio. |
| [diversified_nft_holder.sql](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/diversified_nft_holder.sql) | Wallets that are currently holding at least 5 collections. |


## How do I use this dbt package?

### Prerequisites

To use this dbt project, you must have the following:

* Use Datawaves ETL Cloud to sync data into your destination.
* A PostgreSQL, Databricks destination.

### Install the package

Include in your packages.yml:

```yml
packages:
  - git: "https://github.com/datawaves-xyz/dbt_datawaves"
    revision: "0.0.1"
```
