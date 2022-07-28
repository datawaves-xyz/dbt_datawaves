with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

volume_info as (
  select
    nft_contract_address,
    sum(eth_amount) as volume
  from nft_trades
  where to_date(block_time) >= date_sub(current_date(), 30)
  group by nft_contract_address
  order by volume desc
  limit 30
),

floor_price_info as (
  select distinct
    nft_contract_address,
    floor_price
  from (
    select
      dt,
      nft_contract_address,
      floor_price,
      row_number()over(partition by nft_contract_address order by dt desc) as rank
    from (
      select
        to_date(block_time) as dt,
        nft_contract_address,
        percentile(eth_amount, 0.05) as floor_price
      from nft_trades
      group by dt, nft_contract_address
    )
  )
  where rank = 1
)

select distinct volume_info.nft_contract_address
from volume_info
left join floor_price_info
  on volume_info.nft_contract_address = floor_price_info.nft_contract_address
where floor_price_info.floor_price > 2
