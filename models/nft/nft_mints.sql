{{config(alias='mints')}}

  select *
  from {{ ref("nft_ethereum_mints") }}