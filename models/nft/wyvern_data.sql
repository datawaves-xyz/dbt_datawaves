with wyvern_atomic_match as (
  select *
  from {{ var('wyvern_atomic_match') }}
),

tx as (
  select *
  from {{ var('transactions') }}
),

converted as (
  select
    dt,
    call_tx_hash as tx_hash,
    call_block_number as block_number,
    call_block_time as block_time,
    addrs[1] as buyer,
    {{ binary_to_address(substring('calldatabuy', 49, 20)) }} as buyer_when_aggr,
    addrs[8] as seller,
    uints[4] as original_amount,
    addrs[6] as original_currency_address,
    case
      when {{ substring('calldatabuy', 1, 4) }} in ({{ binary_literal('68f0bcaa') }}) then 'Bundle Trade'
      else 'Single Item Trade'
    end as trade_type,
    case
      when {{ substring('calldatabuy', 1, 4) }} in ({{ binary_literal('fb16a595') }}, {{ binary_literal('23b872dd') }})
        then 'erc721'
      when {{ substring('calldatabuy', 1, 4) }} in ({{ binary_literal('23b872dd') }}, {{ binary_literal('f242432a') }})
        then 'erc1155'
    end as erc_standard,
    addrs[0] as exchange_contract_address,
    case
      when {{ substring('calldatabuy', 1, 4) }} in ({{ binary_literal('fb16a595') }}, {{ binary_literal('96809f90') }})
        then {{ binary_to_address(substring('calldatabuy', 81, 20)) }}
      else addrs[4]
    end as nft_contract_address,
    case
      when addrs[6] = '0x0000000000000000000000000000000000000000' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      else addrs[6]
    end as currency_token,
    case
      when {{ substring('calldatabuy', 1, 4) }} in ({{ binary_literal('fb16a595') }}, {{ binary_literal('96809f90') }})
        then cast({{ binary_to_numeric(substring('calldatabuy', 101, 32)) }} as {{ dbt_utils.type_string() }})
      when substring(calldatabuy, 1, 4) in ({{ binary_literal('23b872dd') }}, {{ binary_literal('f242432a') }})
        then cast({{ binary_to_numeric(substring('calldatabuy', 69, 32)) }} as {{ dbt_utils.type_string() }})
    end as token_id

  from wyvern_atomic_match
  where
    (addrs[3] = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
      or addrs[10] = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073')
    and call_success = true
)

select
  converted.dt,
  converted.tx_hash,
  converted.block_time,
  converted.block_number,
  converted.buyer,
  converted.buyer_when_aggr,
  converted.seller,
  converted.original_amount,
  converted.original_currency_address,
  converted.trade_type,
  converted.erc_standard,
  converted.exchange_contract_address,
  converted.nft_contract_address,
  converted.currency_token,
  converted.token_id,
  tx.from_address as tx_from,
  tx.to_address as tx_to

from converted

left join tx on converted.tx_hash = tx.hash
