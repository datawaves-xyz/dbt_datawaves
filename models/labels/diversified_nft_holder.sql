with contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
),

erc721_transfer as (
  select
    contract_address as nft_contract_address,
    token_id as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ source('ethereum_common', 'erc_721_evt_transfer') }}
),

erc1155_transfer as (
  select
    contract_address as nft_contract_address,
    id as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_single') }}
  {# union distinct
  select
    contract_address as nft_contract_address,
    explode(ids) as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_batch') }}   #}
),

cryptopunks_transfer as (
  select
    contract_address as nft_contract_address,
    punk_index as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_punk_transfer') }}
  union distinct
  select
    contract_address as nft_contract_address,
    punk_index as nft_token_id,
    to_address,
    evt_block_time as block_time
  from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_punk_bought') }}
  union distinct
  select
    contract_address as nft_contract_address,
    punk_index as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_assign') }}
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
      select * from erc721_transfer
      union distinct
      select * from erc1155_transfer
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
