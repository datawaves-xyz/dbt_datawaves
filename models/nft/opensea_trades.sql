with wyvern_data as (
  select *
  from {{ ref('wyvern_data') }}
),

erc20 as (
  select *
  from {{ ref('erc20_tokens') }}
)

select
  wyvern_data.dt,
  wyvern_data.block_time,
  wyvern_data.block_number,
  wyvern_data.tx_hash,
  wyvern_data.buyer,
  wyvern_data.seller,
  wyvern_data.token_id,
  wyvern_data.exchange_contract_address,
  wyvern_data.nft_contract_address,
  wyvern_data.original_currency_address,
  wyvern_data.original_amount as original_amount_raw,
  {{ displayed_amount('wyvern_data.original_amount', 'erc20.decimals') }} as original_amount,
  case
    when wyvern_data.original_currency_address = {{ binary_literal('0000000000000000000000000000000000000000') }}
      then 'ETH'
    else erc20.symbol
  end as original_currency_symbol,
  wyvern_data.currency_token
from wyvern_data
left join erc20 on erc20.contract_address = wyvern_data.nft_contract_address
