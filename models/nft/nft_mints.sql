
with mints as (
  select *
  from {{ ref("nft_ethereum_erc721_mints") }}

  union all

  select *
  from {{ ref("nft_ethereum_erc1155_mints") }}
)

select *
from mints
