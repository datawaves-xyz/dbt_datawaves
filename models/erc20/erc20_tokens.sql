with contracts as (
  select *
  from {{ var('contracts') }}
),

tokens as (
  select *
  from {{ ref('stg_tokens') }}
),

erc20 as (
  select *
  from contracts where is_erc20
)

select
  tokens.contract_address,
  tokens.symbol,
  tokens.name,
  tokens.decimals,
  tokens.total_supply
from tokens

inner join erc20 on erc20.address = tokens.contract_address
