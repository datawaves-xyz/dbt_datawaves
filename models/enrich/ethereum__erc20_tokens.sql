with tokens as (
  select *
  from {{ var('erc20_tokens') }}
)

select
  tokens.address as contract_address,
  tokens.symbol,
  tokens.name,
  cast(tokens.decimals as {{ dbt_utils.type_numeric() }}) as decimals,
  cast(tokens.total_supply as {{ dbt_utils.type_numeric() }}) as total_supply
from tokens
