{{config(alias='transfers')}}

with nft_transfers as (
  select 'erc1155' as erc_standard, *
  from {{ ref('erc1155_ethereum_transfers') }}

  union all

  select 'erc721' as erc_standard, *
  from {{ ref('erc721_ethereum_transfers') }}
)

select
  'Ethereum' as blockchain,
  case
    when `from` = '0x0000000000000000000000000000000000000000' then 'Mint'
    when `to` = '0x0000000000000000000000000000000000000000' 
      or `to` = '0x000000000000000000000000000000000000dead' then 'Burn' 
    else 'Trade' 
  end as `type`,
  *
from nft_transfers