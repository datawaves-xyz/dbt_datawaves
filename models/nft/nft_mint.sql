{{
  cte_import([
    ('transactions', 'stg_transactions'),
    ('traces', 'stg_traces')
  ])
}},

erc721_token_transfers as (
  select *
  from {{ ref('ERC721_evt_Transfer') }}
  where `from` = '0x0000000000000000000000000000000000000000'
    and dt >= '{{ var("start_ts") }}'
    and dt < '{{ var("end_ts") }}'
),

erc721_mint_tx as (
  select
    a.hash as tx_hash,
    b.contract_address as nft_contract_address,
    b.tokenId as nft_token_id,
    b.evt_block_time,
    b.dt,
    b.to as minter,
    sum(a.value) - sum(case when c.value is null then 0 else c.value end) as value
  from transactions as a
  join erc721_token_transfers as b
   on a.hash = b.evt_tx_hash
  left join (
    select
      transaction_hash,
      from_address,
      to_address,
      value
    from traces
    where status = 1
  ) as c
   on a.hash = c.transaction_hash and a.from_address = c.to_address and a.to_address = c.from_address
  group by a.hash, b.contract_address, b.tokenId, b.evt_block_time, b.dt, b.to
),

avg_price_tx as (
  select
    tx_hash,
    avg(value) as avg_value,
    count(distinct nft_token_id) as num_of_items
  from erc721_mint_tx
  group by tx_hash
)

select
  x.tx_hash,
  x.nft_contract_address,
  x.nft_token_id,
  x.evt_block_time,
  x.dt,
  x.minter,
  y.avg_value / y.num_of_items / power(10, 18) as mint_price_eth
from erc721_mint_tx as x
left join avg_price_tx as y
 on x.tx_hash = y.tx_hash
