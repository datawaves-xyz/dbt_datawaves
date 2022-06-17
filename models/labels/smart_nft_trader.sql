with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
),

trader as (
  select
    address,
    sum(amount) as profit
  from (
    select
      seller as address,
      eth_amount as amount
    from nft_trades

    union all
    select
      buyer as address,
      -eth_amount as amount
    from nft_trades
  )
  group by address
),

smart_trader as (
  select
    trader.address,
    trader.profit
  from trader
  left anti join contracts
    on trader.address = contracts.address
  order by trader.profit desc
  limit 100
)

select
  address,
  'Smart NFT Trader' as label,
  'Smart Money' as label_type
from smart_trader
