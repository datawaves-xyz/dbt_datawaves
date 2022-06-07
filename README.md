# dbt_ethereum


## Models


### NFT


| **model**                                                                                                 | **description**                                                                 |
|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [nft_trades](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/nft/nft_trades.sql) | Each record represents a trade in OpenSea/CryptoPunks, enriched with data about the trade. |
| [nft_mints](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/nft/nft_mints.sql) | Each record represents an ERC721/ERC1155 token that has been minted |


### Labels

| **model**                                                                                                 | **description**                                                                 |
|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [nft_whale](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/nft_whale.sql) |  |
| [smart_nft_holder](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_holder.sql) |  |
| [smart_nft_trader](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_trader.sql) |  |
| [smart_nft_sweeper](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/labels/smart_nft_sweeper.sql) |  |

## Development Instructions


Install dbt (with Spark adapter):

```bash
pip install 'dbt-spark[PyHive]'
```

Install sqlfluff (for syntax check):

```bash
pip install sqlfluff
pip install sqlfluff-templater-dbt
```

Run integration tests for utils:

```bash
cd integration_tests
dbt seed
dbt run --models ./models/utils
dbt test
```

## Operational Instructions

Execute a node + any upstream nodes. It is useful when testing your models:

```bash
dbt run --select +{MODEL} --target beta
```

Backfill the history/single date of an incremental model:

```bash
dbt run --select {MODEL} --full-refresh --target prod
dbt run --select {MODEL}  --vars '{"start_ts": "2022-01-01", "end_ts": "2022-01-02"}'  --target prod
```

Refresh external tables:

```bash
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
```
