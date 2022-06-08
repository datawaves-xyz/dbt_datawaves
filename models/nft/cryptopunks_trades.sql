{{
  cte_import([
    ('agg', 'aggregators'),
    ('cryptopunksmarket_evt_punkbought', 'cryptopunks_CryptoPunksMarket_evt_PunkBought'),
    ('cryptopunksmarket_evt_punkbidentered','cryptopunks_CryptoPunksMarket_evt_PunkBidEntered')
  ])
}},

erc20_token_transfers as (
  select
    evt_tx_hash,
    `from` as from_address,
    `to` as to_address
  from {{ ref('ERC20_evt_Transfer') }}
  where dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
    and contract_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
),

prices_usd as (
  select *
  from {{ var('prices_usd') }}
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
    a.punkindex as nft_token_id,
    a.fromaddress as from_address,
    cast(a.value as double) as value,
    case
      when a.toaddress = '0x0000000000000000000000000000000000000000' then b.to_address else a.toaddress
    end as to_address
  from cryptopunksmarket_evt_punkbought a
  left join erc20_token_transfers b
    on a.evt_tx_hash = b.evt_tx_hash and a.fromaddress = b.from_address
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
    left join cryptopunksmarket_evt_punkbidentered b
      on a.nft_token_id = b.punkindex and b.fromaddress = a.to_address and b.evt_block_time <= a.block_time
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
  c.from_address as tx_from,
  c.to_address as tx_to,
  a.dt
from punk_trade a
left join punk_agg_tx b
  on a.tx_hash = b.tx_hash
left join ethereum.transactions c
  on a.tx_hash = c.hash and a.dt = c.dt
left join prices_usd p
  on p.minute = {{ dbt_utils.date_trunc('minute', 'a.block_time') }}
left join agg
  on agg.contract_address = c.to_address
