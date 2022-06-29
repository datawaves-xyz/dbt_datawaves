{{
  cte_import([
    ('agg', 'aggregators'),
    ('nft_tokens', 'nft_tokens')
  ])
}},

tokens as (
  select * 
  from {{ source('ethereum', 'tokens') }}
)

erc721_token_transfers as (
  select *
  from {{ source('ethereum_common', 'erc_721_evt_transfer') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

erc1155_token_transfers as (
  select *
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_single') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

wyvern_data as (
  select *
  from {{ ref('opensea_wyvern_atomic_match') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

prices_usd as (
  select *
  from {{ source('prices', 'usd') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

-- Count number of token IDs in each transaction
erc721_tokens_in_tx as (
  select
    evt_tx_hash as tx_hash,
    tokenId as token_id,
    count(1) as num_of_items
  from erc721_token_transfers
  where `from` != '0x0000000000000000000000000000000000000000'
  group by evt_tx_hash, tokenId
),

-- Count number of token IDs in each transaction
erc1155_tokens_in_tx as (
  select
    evt_tx_hash as tx_hash,
    id as token_id,
    count(1) as num_of_items
  from erc1155_token_transfers
  where `from` != '0x0000000000000000000000000000000000000000'
  group by evt_tx_hash, id
),

-- Count number of token transfers in each transaction;
-- We use this to count number of erc721 and erc1155 transfers
transfers_in_tx as (
  select
    evt_tx_hash as tx_hash,
    count(1) as num_of_transfers
  from erc721_token_transfers
  where `from` != '0x0000000000000000000000000000000000000000'
  group by evt_tx_hash
  union
  select
    evt_tx_hash as tx_hash,
    count(1) as num_of_transfers
  from erc1155_token_transfers
  where `from` != '0x0000000000000000000000000000000000000000'
  group by evt_tx_hash
)

select
  w.token_id as nft_token_id,
  w.exchange_contract_address,
  w.nft_contract_address,
  case
    when erc1155_tokens_in_tx.num_of_items >= 1 then 'erc1155'
    when erc721_tokens_in_tx.num_of_items >= 1 then 'erc721'
    else w.erc_standard
  end as erc_standard,
  agg.name as aggregator,
  -- Count the number of items for different trade types
  case
    when agg.name is null and erc721_tokens_in_tx.num_of_items > 1 then erc721_tokens_in_tx.num_of_items
    when agg.name is null and erc1155_tokens_in_tx.num_of_items > 1 then erc1155_tokens_in_tx.num_of_items
    when w.trade_type = 'Single Item Trade' then 1
    when w.erc_standard = 'erc1155' then erc721_tokens_in_tx.num_of_items
    when w.erc_standard = 'erc721' then erc1155_tokens_in_tx.num_of_items
    else transfers_in_tx.num_of_transfers
  end as number_of_items,
  -- A bundle trade contains at least one erc721 or erc1155 tokens in tx
  case
    when agg.name is not null then 'Aggregator Trade'
    when erc721_tokens_in_tx.num_of_items = 1 or erc1155_tokens_in_tx.num_of_items = 1 then 'Single Item Trade'
    when erc721_tokens_in_tx.num_of_items > 1 or erc1155_tokens_in_tx.num_of_items > 1 then 'Bundle Trade'
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
    else nft_tokens.name
  end as nft_project_name,
  -- Adjust the currency amount/symbol with erc20 tokens
  {{ datawaves_utils.displayed_amount('w.currency_amount', 'p.decimals') }} as currency_amount,
  {{ datawaves_utils.displayed_amount('w.currency_amount', 'p.decimals') }} * p.price as usd_amount,
  {{ datawaves_utils.displayed_amount('w.currency_amount', 'p.decimals') }} * p.price / pe.price as eth_amount,
  w.currency_amount as original_currency_amount,
  case
    when w.original_currency_contract = '0x0000000000000000000000000000000000000000'
      then 'ETH'
    else erc20.symbol
  end as currency_symbol,
  w.currency_contract,
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

left join erc1155_tokens_in_tx
  on erc1155_tokens_in_tx.tx_hash = w.tx_hash
    and erc1155_tokens_in_tx.token_id = w.token_id

left join transfers_in_tx on transfers_in_tx.tx_hash = w.tx_hash

left join prices_usd p
  on p.minute = {{ dbt_utils.date_trunc('minute', 'w.block_time') }}
    and p.contract_address = w.currency_contract

left join prices_usd pe
  on pe.minute = {{ dbt_utils.date_trunc('minute', 'w.block_time') }}
    and pe.symbol = 'WETH'

left join tokens erc20 on erc20.contract_address = w.currency_contract
left join nft_tokens on nft_tokens.contract_address = w.nft_contract_address
left join tokens agg_tokens on agg_tokens.contract_address = w.nft_contract_address
left join agg on agg.contract_address = w.tx_to
