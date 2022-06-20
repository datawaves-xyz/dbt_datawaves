with txn as (
  select *
  from {{ source('ethereum', 'transactions')}}
),

ens_reverse_registrar_1 as (
  select *
  from {{ source('ens', 'reverseregistrar1_call_setname') }}
),

ens_reverse_registrar_2 as (
  select *
  from {{ source('ens', 'reverseregistrar2_call_setname') }}
),

ens_txn as (
  select distinct
    from_address as eth_addr, 
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
  select distinct
    name as ens_name, 
    call_block_number as block_number, 
    call_tx_hash as tx_hash
  from ens_reverse_registrar_2
  -- Only successful transactions
  where call_success is True
  
  union all

  select 
    distinct name as ens_name, 
    call_block_number as block_number, 
    call_tx_hash as tx_hash
  from ens_reverse_registrar_1
  -- Only successful transactions
  where call_success is True
)

select 
  t.eth_addr as address, 
  lower(a.ens_name) as label,
  'ENS Name Reverse' as label_type
from ens_txn as t
inner join all_reverse_registrar as a 
  on a.block_number = t.block_number 
  and a.tx_hash = t.tx_hash 
  and a.ens_name <> '0x0000000000000000000000000000000000000000'
-- Filter all possible Unicode Whitespace characters to prevent malicious issues in the future
WHERE lower(a.ens_name) not regexp '[\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202f\u205f\u3000]'
