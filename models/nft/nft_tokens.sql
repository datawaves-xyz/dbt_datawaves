with nft_tokens as (
  select *
  from {{ source('nft_metadata', 'tokens') }}
)

select
  contract_address,
  name,
  symbol,
  standard
from nft_tokens
