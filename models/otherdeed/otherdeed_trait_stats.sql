with trait_type_info as (
  select distinct
    token_id,
    category as trait,
    'category' as trait_type
  from ethereum_nft_metadata.otherdeed
  where category is not null
  union
  select distinct
    token_id,
    artifact as trait,
    'artifact' as trait_type
  from ethereum_nft_metadata.otherdeed
  where artifact is not null
  union
  select distinct
    token_id,
    sediment as trait,
    'sediment' as trait_type
  from ethereum_nft_metadata.otherdeed
  where sediment is not null
  union
  select distinct
    token_id,
    environment as trait,
    'environment' as trait_type
  from ethereum_nft_metadata.otherdeed
  where environment is not null
  union
  select distinct
    token_id,
    eastern as trait,
    'eastern' as trait_type
  from ethereum_nft_metadata.otherdeed
  where eastern is not null
  union
  select distinct
    token_id,
    southern as trait,
    'southern' as trait_type
  from ethereum_nft_metadata.otherdeed
  where southern is not null
  union
  select distinct
    token_id,
    western as trait,
    'western' as trait_type
  from ethereum_nft_metadata.otherdeed
  where western is not null
  union
  select distinct
    token_id,
    northern as trait,
    'northern' as trait_type
  from ethereum_nft_metadata.otherdeed
  where northern is not null
  union
  select distinct
    token_id,
    koda_core as trait,
    'koda_core' as trait_type
  from ethereum_nft_metadata.otherdeed
  where koda_core is not null
  union
  select distinct
    token_id,
    koda_head as trait,
    'koda_head' as trait_type
  from ethereum_nft_metadata.otherdeed
  where koda_head is not null
  union
  select distinct
    token_id,
    koda_eyes as trait,
    'koda_eyes' as trait_type
  from ethereum_nft_metadata.otherdeed
  where koda_eyes is not null
  union
  select distinct
    token_id,
    koda_clothing as trait,
    'koda_clothing' as trait_type
  from ethereum_nft_metadata.otherdeed
  where koda_clothing is not null
  union
  select distinct
    token_id,
    koda_weapon as trait,
    'koda_weapon' as trait_type
  from ethereum_nft_metadata.otherdeed
  where koda_weapon is not null
),

trade_info as (
  select
    nft_token_id,
    avg(eth_amount) as avg_sale_eth
  from ethereum_nft.nft_trades
  where nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
  group by nft_token_id
),


7d_trader as (
  select distinct
    nft_token_id,
    buyer
  from ethereum_nft.nft_trades
  where nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
    and to_date(block_time) >= date_sub(current_date(), 7)
),

whales as (
  select distinct address
  from ethereum_labels.labels
  where label = 'NFT Millionaire'
),

buyer_info as (
  select
    a.nft_token_id,
    a.buyer,
    b.address as whale_buyer
  from 7d_trader a
  left join whales b
    on a.buyer = b.address
),

recent_sales as (
  select
    nft_token_id,
    eth_amount
  from ethereum_nft.nft_trades
  where nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
    and to_date(block_time) = date_sub(current_date(), 2)
)

select
  a.trait,
  a.trait_type,
  count(distinct a.token_id) as total,
  count(distinct b.buyer) as buyer_7d,
  count(distinct b.whale_buyer) as whale_buyer_7d,
  avg(c.avg_sale_eth) as avg_eth,
  percentile(s.eth_amount, 0.05) as floor_price_eth

from trait_type_info a

left join buyer_info b
  on a.token_id = b.nft_token_id

left join trade_info c
  on a.token_id = c.nft_token_id

left join recent_sales s
  on a.token_id = s.nft_token_id

group by a.trait, a.trait_type
order by whale_buyer_7d desc
