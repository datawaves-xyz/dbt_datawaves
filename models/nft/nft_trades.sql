{{config(alias='trades')}}

  select *
  from {{ ref('opensea_trades') }}

  union all

  select *
  from {{ ref('cryptopunks_trades') }}