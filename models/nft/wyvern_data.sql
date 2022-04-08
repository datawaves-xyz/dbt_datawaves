SELECT
  dt,
  call_tx_hash,
  call_block_number,
  (CASE
    WHEN lower(hex(substring(calldatabuy, 1, 4))) IN ('fb16a595', '96809f90')
      THEN cast(conv(hex(substring(calldatabuy, 101, 32)), 16, 10) AS string)
    WHEN lower(hex(substring(calldatabuy, 1, 4))) IN ('23b872dd', 'f242432a')
      THEN cast(conv(hex(substring(calldatabuy, 69, 32)), 16, 10) AS string)
    END) AS token_id,
  (CASE
      WHEN lower(hex(substring(calldatabuy, 1, 4))) IN ("fb16a595", "96809f90")
        THEN concat("0x", lower(hex(substring(calldatabuy, 81, 20))))
      ELSE addrs[4]
    END) AS nft_contract,
  addrs[1] AS buyer,
  addrs[8] AS seller,
  addrs[6] AS original_currency_contract,
  uints[4] AS original_amount_raw,
  (CASE WHEN addrs[6] = '0x0000000000000000000000000000000000000000'
        THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      ELSE addrs[6]
    END) AS currency_token
FROM {{ var('wyvern_atomic_match') }}
