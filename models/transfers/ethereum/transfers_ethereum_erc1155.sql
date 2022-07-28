with erc1155_ids_batch as (
  select
    *
    explode(ids) as exploded_id,
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_batch') }} 
),

erc1155_values_batch as (
    select
    *
    explode(values) as exploded_value,
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_batch') }} 
)

erc1155_transfers_batch as (
  select
    distinct erc1155_ids_batch.exploded_id,
    erc1155_values_batch.exploded_value,
    erc1155_ids_batch.to,
    erc1155_ids_batch.from.
    erc1155_ids_batch.contract_address,
    erc1155_ids_batch.evt_block_time
  from erc1155_ids_batch
    join erc1155_values_batch
      on erc1155_ids_batch.evt_tx_hash = erc1155_values_batch.evt_tx_hash
        and erc1155_ids_batch.evt_index = erc1155_values_batch.evt_index
),

erc1155_transfers_single as (
  select *
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_single') }}
)

received_transfers as (
  select
    to as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    id as token_id,
    value as amount
  from erc1155_transfers_single

  union

  select 
    to as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    exploded_id as token_id,
    exploded_value as amount
  from erc1155_transfers_batch
) 

sent_transfers as (
  select
    from as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    id as token_id,
    -value as amount
  from erc1155_transfers_single

  union

  select 
    from as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    exploded_id as token_id,
    -exploded_value as amount
  from erc1155_transfers_batch
),

transfers as (
  select *
  from sent_transfers

  union

  select *
  from received_transfers
)

select 'Ethereum' as blockchain, *
from transfers