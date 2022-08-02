
with erc1155_transfers_batch as (
  select
    distinct erc1155_ids_batch.exploded_id,
    erc1155_values_batch.exploded_value,
    erc1155_ids_batch.to,
    erc1155_ids_batch.from,
    erc1155_ids_batch.contract_address,
    erc1155_ids_batch.evt_block_time,
    erc1155_ids_batch.evt_tx_hash
  from (select *, explode(ids) as exploded_id
        from {{ source('ethereum_common', 'erc_1155_evt_transfer_batch') }}) erc1155_ids_batch
    join (select *, explode(values) as exploded_value
          from {{ source('ethereum_common', 'erc_1155_evt_transfer_batch') }}) erc1155_values_batch
      on erc1155_ids_batch.evt_tx_hash = erc1155_values_batch.evt_tx_hash
        and erc1155_ids_batch.evt_index = erc1155_values_batch.evt_index
),

erc1155_transfers as (
  select
    from,
    to,
    id as token_id,
    value as amount,
    contract_address as token_address,
    evt_block_time as block_time,
    evt_tx_hash as tx_hash
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_single') }}

  union all

  select
    from,
    to,
    contract_address as token_address,
    exploded_id as token_id,
    exploded_value as amount,
    evt_block_time as block_time,
    evt_tx_hash as tx_hash    
  from erc1155_transfers_batch
),

erc721_tansfers as (
  select
    from,
    to,
    id as token_id,
    1 as amount,
    contract_address as token_address,
    evt_block_time as block_time,
    evt_tx_hash as tx_hash
  from {{ source('ethereum_common', 'erc_721_evt_transfer') }}
),


transfers as (
  select 'erc1155' as erc_standard, *
  from erc1155_transfers

  union all

  select 'erc721' as erc_standard, *
  from erc721_transfers
)

select 'Ethereum' as blockchain, *
from transfers