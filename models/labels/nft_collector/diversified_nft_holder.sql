with contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
),

nft_transfer as (
  select
    token_address as nft_contract_address,
    token_id as nft_token_id,
    to as to_address,
    block_time
  from {{ ref("nft_ethereum_transfers") }}
),

cryptopunks_transfer as (
  select *
  from {{ ref('cryptopunks_ethereum_transfers') }}
), 

holder_info as (
  select distinct
    nft_contract_address,
    nft_token_id,
    to_address as holder
  from (
    select
      nft_contract_address,
      nft_token_id,
      to_address,
      row_number()over(partition by nft_contract_address, nft_token_id order by block_time desc) as rank 
    from (
      select * from nft_transfer
      union distinct
      select * from cryptopunks_transfer
    )
    where to_address != '0x0000000000000000000000000000000000000000'
  ) as a
  left anti join contracts b
  on a.to_address = b.address
  where rank = 1
)

select
  holder as address,
  'Diversified NFT Holder' as label,
  'NFT Collector' as label_type
from (
  select
    holder,
    count(distinct nft_contract_address) as collection_count
  from holder_info
  group by holder
)
where collection_count > 4
