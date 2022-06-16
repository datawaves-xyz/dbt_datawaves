with nft_token_metadata as (
  select *
  from {{ source('nft_metadata', 'token_metadata') }}
)

select
  token_address as contract_address,
  token_id,
  amount,
  token_hash,
  block_number_minted,
  contract_type,
  name,
  symbol,
  token_uri,
  attributes,
  synced_at
from nft_token_metadata
