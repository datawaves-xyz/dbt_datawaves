with opensea_trades as (
  select *
  from {{ ref('nft_trades') }}
  where platform = 'OpenSea'
),

contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
)

select distinct
  a.address,
  'OpenSea Trader' as label,
  'NFT Collector' as label_type
from (
  select distinct seller as address
  from opensea_trades
  union
  select distinct buyer as address
  from opensea_trades
) as a
left anti join contracts
  on a.address = contracts.address
