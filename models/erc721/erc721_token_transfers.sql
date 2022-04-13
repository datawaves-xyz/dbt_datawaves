with contracts as (
  select *
  from {{ var('contracts') }}
),

token_transfers as (
  select *
  from {{ var('token_transfers') }}
),

erc721 as (
  select *
  from contracts where is_erc721
),

renamed as (
  select
    token_transfers.token_address,
    token_transfers.from_address,
    token_transfers.to_address,
    token_transfers.value as token_id,
    token_transfers.transaction_hash,
    token_transfers.log_index,
    token_transfers.block_timestamp,
    token_transfers.block_number,
    token_transfers.block_hash,
    token_transfers.dt
  from token_transfers

  inner join erc721 on erc721.address = token_transfers.token_address
)

select * from renamed
