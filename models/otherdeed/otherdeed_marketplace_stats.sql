with all_inflow as (
  select
    to_date(block_time) as dt,
    sum(usd_amount) as volume
  from ethereum_nft.nft_trades

  where to_date(block_time) >= date_sub(current_date(), 30)
    and nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
  group by dt
),

all_outflow as (
  select
    to_date(block_time) as dt,
    -sum(usd_amount) as volume
  from ethereum_nft.nft_trades

  where to_date(block_time) >= date_sub(current_date(), 30)
    and nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
  group by dt
),

smart_trader_inflow as (
  select
    to_date(t.block_time) as dt,
    sum(t.usd_amount) as volume
  from ethereum_nft.nft_trades t

  inner join ethereum_labels.labels ll
    on t.nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
      and ll.address = t.buyer
      and ll.label = 'Smart NFT Holder'

  where to_date(t.block_time) >= date_sub(current_date(), 30)
  group by dt
),

smart_trader_outflow as (
  select
    to_date(t.block_time) as dt,
    -sum(t.usd_amount) as volume
  from ethereum_nft.nft_trades t

  inner join ethereum_labels.labels ll
    on t.nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
      and ll.address = t.seller
      and ll.label = 'Smart NFT Holder'

  where to_date(t.block_time) >= date_sub(current_date(), 30)
  group by dt
),

whale_inflow as (
  select
    to_date(t.block_time) as dt,
    sum(t.usd_amount) as volume
  from ethereum_nft.nft_trades t

  inner join ethereum_labels.labels ll
    on t.nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
      and ll.address = t.buyer
      and ll.label = 'NFT Millionaire'

  where to_date(t.block_time) >= date_sub(current_date(), 30)
  group by dt
),

whale_outflow as (
  select
    to_date(t.block_time) as dt,
    -sum(t.usd_amount) as volume
  from ethereum_nft.nft_trades t

  inner join ethereum_labels.labels ll
    on t.nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
      and ll.address = t.seller
      and ll.label = 'NFT Millionaire'

  where to_date(t.block_time) >= date_sub(current_date(), 30)
  group by dt
)

select

  all_inflow.dt,
  coalesce(all_inflow.volume, 0) as all_inflow_volume,
  coalesce(all_outflow.volume, 0) as all_outflow_volume,
  coalesce(smart_trader_inflow.volume, 0) as smart_trader_inflow_volume,
  coalesce(smart_trader_outflow.volume, 0) as smart_trader_outflow_volume,
  coalesce(whale_inflow.volume, 0) as whale_inflow_volume,
  coalesce(whale_outflow.volume, 0) as whale_outflow_volume

from all_inflow

left outer join all_outflow on all_inflow.dt = all_outflow.dt
left outer join smart_trader_inflow on all_inflow.dt = smart_trader_inflow.dt
left outer join smart_trader_outflow on all_inflow.dt = smart_trader_outflow.dt
left outer join whale_inflow on all_inflow.dt = whale_inflow.dt
left outer join whale_outflow on all_inflow.dt = whale_outflow.dt
order by all_inflow.dt
