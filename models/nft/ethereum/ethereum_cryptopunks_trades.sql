{{
  cte_import([
    ('agg', 'aggregators'),
  ])
}},

tx as (
  select *
  from {{ source('ethereum', 'transactions') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'  
),

punk_bought as (
  select *
  from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_punk_bought')}}
),

punk_bid_entered as (
  select *
  from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_punk_bid_entered') }}
),

erc20_token_transfers as (
  select
    evt_tx_hash,
    `from` as from_address,
    `to` as to_address
  from {{ source('ethereum_common', 'erc_20_evt_transfer') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
    and contract_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
),

prices_usd as (
  select *
  from {{ source('ethereum', 'prices') }}
  where contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    and dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

address_info as (
  select
    a.dt,
    a.evt_block_time as block_time,
    a.evt_block_number as block_number,
    a.evt_tx_hash as tx_hash,
    a.punk_index as nft_token_id,
    a.from_address as from_address,
    cast(a.value as double) as value,
    case
      when a.to_address = '0x0000000000000000000000000000000000000000' then b.to_address else a.to_address
    end as to_address
  from punk_bought a
  left join erc20_token_transfers b
    on a.evt_tx_hash = b.evt_tx_hash and a.from_address = b.from_address
),

punk_trade as (
  select
    dt,
    block_time,
    block_number,
    tx_hash,
    nft_token_id,
    from_address,
    to_address,
    (case when value = 0 then coalesce(bid_value, 0) else value end) as original_currency_amount
  from (
    select
      a.*,
      cast(b.value as double) as bid_value,
      row_number()over(partition by a.nft_token_id, a.block_time order by b.evt_block_time desc) as rank
    from address_info a
    left join punk_bid_entered b
      on a.nft_token_id = b.punk_index and b.from_address = a.to_address and b.evt_block_time <= a.block_time
  )
  where rank = 1
),

punk_agg_tx as (
  select
    tx_hash,
    block_time,
    block_number,
    count(distinct nft_token_id) as num
  from punk_trade
  group by tx_hash, block_time, block_number
)

select
  'LarvaLabs Contract' as platform,
  a.nft_token_id,
  '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' as exchange_contract_address,
  '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' as nft_contract_address,
  'erc20' as erc_standard,
  b.num as number_of_items,
  agg.name as aggregator,
  case when b.num > 1 then 'Bundle Trade' else 'Single Item Trade' end as trade_type,
  a.from_address as buyer,
  a.to_address as seller,
  'CryptoPunks' as nft_project_name,
  a.original_currency_amount / power(10, 18) as currency_amount,
  'ETH' as currency_symbol,
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_contract,
  a.original_currency_amount / power(10, 18) * p.price as usd_amount,
  a.original_currency_amount / power(10, 18) as eth_amount,
  a.original_currency_amount,
  '0x0000000000000000000000000000000000000000' as original_currency_contract,
  a.block_time,
  a.block_number,
  a.tx_hash,
  tx.from_address as tx_from,
  tx.to_address as tx_to,
  a.dt
from punk_trade a
left join punk_agg_tx b
  on a.tx_hash = b.tx_hash
left join tx
  on a.tx_hash = tx.hash and a.dt = tx.dt
left join prices_usd p
  on p.minute = {{ dbt_utils.date_trunc('minute', 'a.block_time') }}
left join agg
  on agg.contract_address = tx.to_address
