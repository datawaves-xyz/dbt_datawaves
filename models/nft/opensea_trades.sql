with w as (
  select *
  from {{ ref('wyvern_data') }}
),

erc20 as (
  select *
  from {{ ref('erc20_tokens') }}
)

select
  w.dt,
  w.block_number,
  w.tx_hash,
  w.exchange_contract_address,
  w.nft_contract_address,
  w.original_currency_address,
  w.original_amount as original_amount_raw,
  {{ displayed_amount('w.original_amount', 'erc20.decimals') }} as original_amount,
  case when w.original_currency_address = {{ binary_literal('0000000000000000000000000000000000000000') }} then 'ETH'
    else erc20.symbol
  end as original_currency_symbol
from w
left join erc20 on erc20.contract_address = w.nft_contract_address
