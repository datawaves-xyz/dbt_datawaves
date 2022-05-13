with trait_info as (
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

rarity_score as (
  select
    token_id,
    trait,
    trait_type,
    count(case when trait is not null then 1 end) over (partition by trait_type, trait )
    / count(case when trait is not null then 1 end) over (partition by trait_type ) as rarity_score

  from trait_info
),

rarity_scoreboard as (
  select
    token_id,
    sum(ln(rarity_score)) as rarity_score
  from rarity_score
  group by token_id
)

select
  y.*,
  x.nft_token_id,
  x.latest_eth_amount,
  z.rarity_score

from
  (select distinct
    nft_token_id,
    eth_amount as latest_eth_amount
    from (
      select distinct
        nft_token_id,
        eth_amount,
        row_number() over (partition by nft_contract_address, nft_token_id order by block_time desc) as rank_by_time
      from ethereum_nft.nft_trades
      where nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
    )
    where rank_by_time = 1
  ) x

left join ethereum_nft_metadata.otherdeed y
  on x.nft_token_id = y.token_id

left join rarity_scoreboard z
  on x.nft_token_id = z.token_id
