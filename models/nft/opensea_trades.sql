{{
  cte_import([
    ('agg', 'aggregators'),
    ('tokens', 'stg_ethereum__tokens'),
  ])
}},

erc721_token_transfers as (
  select *
  from {{ ref('stg_ethereum__token_transfers') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

wyvern_data as (
  select *
  from {{ ref('wyvern_data') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

prices_usd as (
  select *
  from {{ var('prices_usd') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

-- Count number of token IDs in each transaction
erc721_tokens_in_tx as (
  select
    transaction_hash as tx_hash,
    cast(round(value, 0) as string) as token_id,
    count(1) as num
  from erc721_token_transfers
  where from_address != '0x0000000000000000000000000000000000000000'
  group by transaction_hash, cast(round(value, 0) as string)
),

-- Count number of token transfers in each transaction;
-- We use this to count number of erc721 and erc1155 items when there's no token_id associated
transfers_in_tx as (
  select
    transaction_hash as tx_hash,
    count(1) as num
  from erc721_token_transfers
  where from_address != '0x0000000000000000000000000000000000000000'
  group by transaction_hash
)

select
  w.token_id as nft_token_id,
  w.exchange_contract_address,
  w.nft_contract_address,
  case
    when erc721_tokens_in_tx.num >= 1 then 'erc721'
    else w.erc_standard
  end as erc_standard,
  -- Count the number of items for different trade types
  case
    when agg.name is null and erc721_tokens_in_tx.num > 1 then erc721_tokens_in_tx.num
    when w.trade_type = 'Single Item Trade' then 1
    else transfers_in_tx.num
  end as number_of_items,
  agg.name as aggregator,
  case
    when agg.name is not null then 'Aggregator Trade'
    when agg.name is null and erc721_tokens_in_tx.num = 1 then 'Single Item Trade'
    when agg.name is null and erc721_tokens_in_tx.num > 1 then 'Bundle Trade'
    else w.trade_type
  end as trade_type,
  -- Replace the buyer when using aggregator to trade
  case when agg.name is not null then w.buyer_when_aggr
    else w.buyer
  end as buyer,
  w.seller,
  -- Get the token of aggregator when using aggregator to trade
  case
    when agg.name is not null then agg_tokens.name
    else tokens.name
  end as nft_project_name,
  -- Adjust the currency amount/symbol with erc20 tokens
  {{ displayed_amount('w.currency_amount', 'erc20.decimals') }} as currency_amount,
  {{ displayed_amount('w.currency_amount', 'erc20.decimals') }} * p.price as usd_amount,
  w.currency_amount as original_currency_amount,
  case
    when w.original_currency_contract = '0x0000000000000000000000000000000000000000'
      then 'ETH'
    else erc20.symbol
  end as currency_symbol,
  w.currency_contact,
  w.original_currency_contract,
  -- blocks & tx
  w.block_time,
  w.block_number,
  w.tx_hash,
  w.tx_from,
  w.tx_to,
  -- date partition column
  w.dt
from wyvern_data w

left join erc721_tokens_in_tx
  on erc721_tokens_in_tx.tx_hash = w.tx_hash
    and erc721_tokens_in_tx.token_id = w.token_id

left join transfers_in_tx on transfers_in_tx.tx_hash = w.tx_hash

left join prices_usd p
  on p.minute = {{ dbt_utils.date_trunc('minute', 'w.block_time') }}
    and p.contract_address = w.currency_token

left join tokens erc20 on erc20.contract_address = w.currency_token
left join tokens on tokens.contract_address = w.nft_contract_address
left join tokens agg_tokens on agg_tokens.contract_address = w.nft_contract_address
left join agg on agg.contract_address = w.tx_to
