with txn as (
  select
  * from {{ source('ethereum', 'transactions') }}
),

ens_reverse_registrar as (
  select
  * from {{ source('ethereum_ens', 'reverse_registrar_1_call_set_name') }}
  union
  select
  * from {{ source('ethereum_ens', 'reverse_registrar_2_call_set_name') }}
),

ens_txn as (
  select 
    distinct from_address as eth_addr, 
    block_number, 
    block_timestamp, 
    hash as tx_hash
  from txn 
  where
      -- Old Reverse Registrar and Reverse Registrar contracts
      (to_address = '0x9062c0a6dbd6108336bcbe4593a3d1ce05512069' or to_address = '0x084b1c3c81545d370f3634392de611caabff8148')
      -- Only successful transactions
      and receipt_status = 1
),

all_reverse_registrar as (
  select 
    distinct name as ens_name, 
    call_block_number as block_number, 
    call_tx_hash as tx_hash
  from ens_reverse_registrar
  -- Only successful transactions
  where call_success is True
), 

select 
* from all_reverse_registrar
