with tokens as (
  select *
  from {{ var('tokens') }}
)

select
  tokens.address as contract_address,
  tokens.symbol,
  tokens.name,
  cast(tokens.decimals as {{ dbt_utils.type_int() }}) as decimals,
  cast(tokens.total_supply as {{ dbt_utils.type_int() }}) as total_supply
from tokens
