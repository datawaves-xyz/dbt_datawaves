with contracts as (
  select *
  from {{ var('contracts') }}
),

token_transfers as (
  select *
  from {{ var('token_transfers') }}
),

erc20 as (
  select *
  from contracts where is_erc721
)

select
  token_transfers.token_address,
  token_transfers.from_address,
  token_transfers.to_address,
  token_transfers.value,
  token_transfers.transaction_hash,
  token_transfers.log_index,
  token_transfers.block_timestamp,
  token_transfers.block_number,
  token_transfers.block_hash,
  token_transfers.dt
from token_transfers

inner join erc20 on erc20.address = token_transfers.token_address
