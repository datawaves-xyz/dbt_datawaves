{{
  cte_import([
    ('otherdeed', 'otherdeed'),
    ('nft_trades', 'nft_trades')
  ])
}},

trade_info as (
  select
    nft_token_id,
    avg(eth_amount) as avg_eth_amount,
    max(eth_amount) as max_eth_amount
  from nft_trades
  where nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
  group by nft_token_id
),

token_info as (
  select
    o.token_id,
    o.has_koda,
    t.avg_eth_amount,
    t.max_eth_amount
  from otherdeed o
  left outer join trade_info t
    on t.nft_token_id = o.token_id
)

select
  round(avg(avg_eth_amount), 2) as all_avg_eth_amount,
  round(avg(case when has_koda = true then avg_eth_amount end), 2) as koda_avg_eth_amount,
  round(avg(case when has_koda = false then avg_eth_amount end), 2) as without_koda_avg_eth_amount,
  count(distinct token_id) as all_item,
  count(distinct case when has_koda = true then token_id end) as koda_item,
  count(distinct case when has_koda = false then token_id end) as without_koda_item
from token_info