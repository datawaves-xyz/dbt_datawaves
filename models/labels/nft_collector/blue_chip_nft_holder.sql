with blue_chip as (
  select *
  from {{ ref('blue_chip_index') }}
),

contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
),

nft_transfer as (
  select
    token_address as nft_contract_address,
    token_id as nft_token_id,
    `to` as to_address,
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
  )
  where rank = 1
)

select distinct
  holder_info.holder as address,
  'NFT Blue Chip Holder' as label,
  'NFT Collector' as label_type
from blue_chip
join holder_info
  on blue_chip.nft_contract_address = holder_info.nft_contract_address
left anti join contracts
  on holder_info.holder = contracts.address
