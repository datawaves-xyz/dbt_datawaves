with contracts as (
  select *
  from {{ var('contracts') }}
),

tokens as (
  select *
  from {{ ref('stg_tokens') }}
),

erc721 as (
  select *
  from contracts where is_erc721
)

select
  tokens.contract_address,
  tokens.symbol,
  tokens.name,
  tokens.decimals,
  tokens.total_supply
from tokens

inner join erc721 on erc721.address = tokens.contract_address
