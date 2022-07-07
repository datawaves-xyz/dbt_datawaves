with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

floor_price_info as (
  select
    dt,
    nft_contract_address,
    floor_price
  from (
    select
      nft_contract_address,
      to_date(block_time) as dt,
      percentile(eth_amount, 0.05) as floor_price
    from nft_trades
    where to_date(block_time) >= date_sub(current_date(), 33)
    group by dt, nft_contract_address
  )
),

floor_buy_info as (
  select
    a.dt,
    a.buyer,
    a.nft_contract_address,
    a.nft_token_id,
    a.eth_amount,
    b.floor_price
  from (
    select
      buyer,
      nft_contract_address,
      nft_token_id,
      eth_amount,
      to_date(block_time) as dt
    from nft_trades
    where to_date(block_time) >= date_sub(current_date(), 33)
  ) a
  inner join floor_price_info b
    on a.dt = b.dt and a.nft_contract_address = b.nft_contract_address
),

sweeper_info as (
  select
    dt,
    buyer,
    count(1) as transactions
  from floor_buy_info
  where eth_amount <= floor_price
  group by dt, buyer
  having transactions >= 5 -- top 10%
),

sweeper_stat as (
  select
    buyer,
    sum(sale_amount - purcehase_amount) as profit
  from (
    select
      b.buyer,
      b.nft_contract_address,
      b.nft_token_id,
      coalesce(b.eth_amount, 0) as purcehase_amount,
      coalesce(c.eth_amount, 0) as sale_amount
    from (
      select distinct buyer
      from sweeper_info
    ) as a
    inner join (
      select
        block_time,
        buyer,
        nft_contract_address,
        nft_token_id,
        eth_amount
      from nft_trades
      where to_date(block_time) >= date_sub(current_date(), 33)
    ) as b
    on a.buyer = b.buyer
    left join (
      select
        block_time,
        seller,
        nft_contract_address,
        nft_token_id,
        eth_amount
      from nft_trades
      where to_date(block_time) >= date_sub(current_date(), 30)
    ) as c
    on
      b.buyer = c.seller and b.nft_contract_address = c.nft_contract_address and b.nft_token_id = c.nft_token_id and b.block_time < c.block_time
  )
  group by buyer
)

select
  buyer as address,
  'Smart NFT Sweeper' as label,
  'Smart Money' as label_type
from sweeper_stat
where profit > 0
