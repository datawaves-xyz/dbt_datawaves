{{config(alias='trades')}}

select
  blockchain,
  platform,
  nft_token_id,
  exchange_contract_address,
  nft_contract_address,
  erc_standard,
  aggregator,
  number_of_items,
  trade_type,
  buyer,
  seller,
  nft_project_name,
  currency_amount,
  usd_amount,
  eth_amount,
  original_currency_amount,
  currency_symbol,
  currency_contract,
  original_currency_contract,
  block_time,
  block_number,
  tx_hash,
  tx_from,
  tx_to
from {{ ref('cryptopunks_ethereum_trades') }}