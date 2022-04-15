{{
  cte_import([
    ('agg', 'aggregators'),
    ('tokens', 'stg_tokens'),
  ])
}},

erc721_token_transfers as (
  select *
  from {{ var('token_transfers') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

tx as (
  select *
  from {{ var('transactions') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

wyvern_data as (
  select *
  from {{ ref('wyvern_data') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

-- Count token IDs in each transaction
erc721_tokens_in_tx as (
  select
    w.tx_hash as tx_hash,
    cast(t.value as string) as token_id,
    count(1) as token_count
  from erc721_token_transfers t
  left join wyvern_data w on w.tx_hash = t.transaction_hash
    and w.token_id = cast(t.value as string)
  where t.from_address != '0x0000000000000000000000000000000000000000'
  group by tx_hash, cast(t.value as string)
)

select
  w.token_id as nft_token_id,
  w.exchange_contract_address,
  w.nft_contract_address,
  case
    when erc721_tokens_in_tx.token_count >= 1 then 'erc721'
    else w.erc_standard
  end as erc_standard,
  -- Count the number of items for different trade types
  case
    when agg.name is null and erc721_tokens_in_tx.token_count > 1 then erc721_tokens_in_tx.token_count
    when w.trade_type = 'Single Item Trade' then 1
    else(
      select count(1)
      from erc721_token_transfers t
      where t.transaction_hash = w.tx_hash
        and t.from_address != '0x0000000000000000000000000000000000000000')
  end as number_of_items,
  agg.name as aggregator,
  case
    when agg.name is not null then 'Aggregator Trade'
    when agg.name is null and erc721_tokens_in_tx.token_count = 1 then 'Single Item Trade'
    when agg.name is null and erc721_tokens_in_tx.token_count > 1 then 'Bundle Trade'
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
  w.original_currency_address,
  w.original_amount as original_amount_raw,
  {{ displayed_amount('w.original_amount', 'erc20.decimals') }} as original_amount,
  case
    when w.original_currency_address = {{ binary_to_address('0000000000000000000000000000000000000000') }}
      then 'ETH'
    else erc20.symbol
  end as original_currency_symbol,
  w.currency_token,
  -- blocks & tx
  w.block_time,
  w.block_number,
  w.tx_hash,
  w.dt,
  tx.from_address as tx_from,
  tx.to_address as tx_to
from wyvern_data w

left join tx on w.tx_hash = tx.hash

left join erc721_tokens_in_tx
  on erc721_tokens_in_tx.tx_hash = w.tx_hash
    and erc721_tokens_in_tx.token_id = w.token_id

left join tokens erc20 on erc20.contract_address = w.nft_contract_address
left join tokens on tokens.contract_address = w.nft_contract_address
left join tokens agg_tokens on agg_tokens.contract_address = w.nft_contract_address
left join agg on agg.contract_address = w.contract_address