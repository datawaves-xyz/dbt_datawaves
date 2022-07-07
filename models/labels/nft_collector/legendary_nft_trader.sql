with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
),

trade_info as (
  select
    a.address,
    count(1) as transaction
  from (
    select
      seller as address,
      block_time,
      'sell' as trade_type
    from nft_trades
    union all
    select
      buyer as address,
      block_time,
      'buy' as trade_type
    from nft_trades
  ) as a
  left anti join contracts
  on a.address = contracts.address
  group by a.address
),

top_stat as (
  select round(percentile(transaction, 0.999), 0) as target_value
  from trade_info 
)

select distinct
  trade_info.address,
  'Legendary NFT Trader' as label,
  'NFT Collector' as label_type
from trade_info
full outer join top_stat
where trade_info.transaction >= top_stat.target_value
