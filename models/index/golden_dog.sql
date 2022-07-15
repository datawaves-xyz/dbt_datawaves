with free_mint as (
  select
    nft_contract_address,
    sum(eth_mint_price) as eth_amount
  from {{ ref('nft_mints') }}
  where to_date(block_time) >= date_sub(current_date(), 60)
  group by nft_contract_address
  having eth_amount = 0
),

nft_trades as (
  select *
  from {{ ref('nft_trades') }}
  where to_date(block_time) >= date_sub(current_date(), 60)
),

free_mint_nft_trades as (
  select b.*
  from free_mint as a
  inner join nft_trades as b
    on a.nft_contract_address = b.nft_contract_address
),

volume_stat as (
  select
    nft_contract_address,
    sum(eth_amount) as volume
  from free_mint_nft_trades
  group by nft_contract_address
),

floor_price_stat as (
  select
    nft_contract_address,
    max(floor_price) as max_floor_price
  from (
    select
      to_date(block_time) as dt,
      nft_contract_address,
      percentile(eth_amount, 0.05) as floor_price
    from free_mint_nft_trades
    group by dt, nft_contract_address
  )
  group by nft_contract_address
),

volume_filter as (
  select percentile(volume, 0.975) as volume_top
  from volume_stat
),

floor_price_filter as (
  select percentile(max_floor_price, 0.975) as floor_price_top
  from floor_price_stat
),

final as (
  select
    a.nft_contract_address,
    coalesce(a.volume, 0) as volume,
    coalesce(b.max_floor_price, 0) as max_floor_price,
    c.volume_top,
    d.floor_price_top
  from volume_stat as a
  inner join floor_price_stat as b
    on a.nft_contract_address = b.nft_contract_address
  full outer join volume_filter as c
  full outer join floor_price_filter as d
)

select distinct nft_contract_address
from final
where volume >= volume_top and max_floor_price >= floor_price_top
