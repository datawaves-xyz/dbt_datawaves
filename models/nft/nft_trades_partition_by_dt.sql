{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['dt'],
    file_format='parquet'
  )
}}

with opensea_trades as (
  select *
  from {{ ref('opensea_trades') }}
),

final as (
  select
    'OpenSea' as platform,
    nft_token_id,
    exchange_contract_address,
    nft_contract_address,
    erc_standard,
    number_of_items,
    aggregator,
    trade_type,
    buyer,
    seller,
    nft_project_name,
    original_amount_raw,
    original_amount,
    usd_amount,
    currency_symbol,
    original_currency_address,
    currency_token,
    block_time,
    block_number,
    tx_hash,
    tx_from,
    tx_to,
    dt

  from opensea_trades

  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
)

select /*+ REPARTITION(dt) */ *
from final