# dbt_ethereum


## Models


### NFT


| **model**                                                                                                 | **description**                                                                 |
|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [opensea_trades](https://github.com/datawaves-xyz/dbt_ethereum/blob/master/models/nft/opensea_trades.sql) | Each record represents a trade in OpenSea, enriched with data about the trade. |



## Development


Install dbt (with Spark adapter):

- pip install dbt
- pip install 'dbt-spark[PyHive]'


Install sqlfluff:

- pip install sqlfluff
- pip install sqlfluff-templater-dbt


Try running integration tests for utils:

- cd integration_tests
- dbt seed
- dbt run --models ./models/utils
- dbt test


## Operations

Backfill the history/single date of an incremental model

- dbt run --select {MODEL} --full-refresh --target prod
- dbt run --select {MODEL}  --vars '{"start_ts": "2022-01-01", "end_ts": "2022-01-02"}'  --target prod