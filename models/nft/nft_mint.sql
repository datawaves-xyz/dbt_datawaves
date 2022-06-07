{{
  cte_import([
    ('nft_mint_erc721', 'nft_mint_erc721'),
    ('nft_mint_erc1155', 'nft_mint_erc1155'),
    ('cryptopunksmarket_evt_assign', 'cryptopunks_CryptoPunksMarket_evt_Assign')
  ])
}},

mint_union as (
  select
    tx_hash,
    nft_contract_address,
    nft_token_id,
    1 as quantity,
    evt_block_time,
    dt,
    minter,
    eth_mint_price,
    usd_mint_price,
    'erc721' as erc_standard
  from nft_mint_erc721

  union all
  select
    tx_hash,
    nft_contract_address,
    nft_token_id,
    quantity,
    evt_block_time,
    dt,
    minter,
    eth_mint_price,
    usd_mint_price,
    'erc_1155' as erc_standard
  from nft_mint_erc1155

  union all
  select
    evt_tx_hash as tx_hash,
    '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' as nft_contract_address,
    punkIndex as nft_token_id,
    1 as quantity,
    evt_block_time,
    dt,
    `to` as minter,
    0 as eth_mint_price,
    0 as usd_mint_price,
    'erc20' as erc_standard
  from cryptopunksmarket_evt_assign
)

select *
from mint_union
