{{
  cte_import([
    ('nft_whale', 'nft_whale'),
    ('smart_nft_trader', 'smart_nft_trader'),
    ('smart_nft_holder', 'smart_nft_holder'),
    ('smart_nft_sweeper', 'smart_nft_sweeper'),
    ('smart_nft_minter', 'smart_nft_minter'),
    ('smart_nft_golden_dog_minter', 'smart_nft_golden_dog_minter'),
    ('opensea_trader', 'opensea_trader'),
    ('legendary_nft_trader', 'legendary_nft_trader'),
    ('epic_nft_trader', 'epic_nft_trader'),
    ('rare_nft_trader', 'rare_nft_trader'),
    ('uncommon_nft_trader', 'uncommon_nft_trader'),
    ('diversified_nft_holder', 'diversified_nft_holder'),
    ('blue_chip_nft_holder', 'blue_chip_nft_holder'),
    ('ens_name', 'ens_name'),
    ('ens_name_reverse', 'ens_name_reverse')
  ])
}}

-- Whale
select address, label, label_type from nft_whale

-- Smart Money
union all
select address, label, label_type from smart_nft_trader
union all
select address, label, label_type from smart_nft_holder
union all
select address, label, label_type from smart_nft_sweeper
union all
select address, label, label_type from smart_nft_minter
union all
select address, label, label_type from smart_nft_golden_dog_minter

-- NFT collecter
union all
select address, label, label_type from opensea_trader
union all
select address, label, label_type from legendary_nft_trader
union all
select address, label, label_type from epic_nft_trader
union all
select address, label, label_type from rare_nft_trader
union all
select address, label, label_type from uncommon_nft_trader
union all
select address, label, label_type from diversified_nft_holder
union all
select address, label, label_type from blue_chip_nft_holder

-- ENS
union all
select address, label, label_type from ens_name
union all
select address, label, label_type from ens_name_reverse
