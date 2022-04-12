with contracts as (
  select *
  from {{ var('contracts') }}
),

tokens as (
  select *
  from {{ var('tokens') }}
),

erc721 as (
  select *
  from contracts where is_erc721
)

select
  tokens.address as contract_address,
  tokens.symbol,
  tokens.name,
  cast(tokens.decimals as {{ dbt_utils.type_int() }}) as decimals,
  cast(tokens.total_supply as {{ dbt_utils.type_int() }}) as total_supply
from tokens

inner join erc721 on erc721.address = tokens.address
