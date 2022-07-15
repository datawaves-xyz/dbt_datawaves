with mint_info as (
  select *
  from {{ ref('nft_mints') }}
  where to_date(block_time) >= date_sub(current_date(), 60)
),

trade_info as (
  select *
  from {{ ref('nft_trades') }}
  where to_date(block_time) >= date_sub(current_date(), 60)
),

mint_stat as (
  select
    a.minter,
    sum(a.eth_mint_price) as mint_amount,
    sum(b.eth_amount - a.eth_mint_price) as profit,
    count(distinct a.nft_contract_address) as mint_collection_count
  from mint_info as a
  inner join trade_info as b
    on a.minter = b.seller and a.nft_contract_address = b.nft_contract_address and a.nft_token_id = b.nft_token_id
  group by a.minter
  order by profit desc
)

select distinct
  minter as address,
  'Smart NFT Minter' as label,
  'Smart Money' as label_type
from mint_stat
where mint_collection_count >= 2
  and ((mint_amount > 0 and profit / mint_amount >= 5) or (mint_amount = 0 and profit > 0))
limit 100
