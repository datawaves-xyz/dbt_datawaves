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
    ('ens_name_reverse', 'ens_name_reverse'),
    ('phish_hacker', 'phish_hacker'),
  ])
}}

select * from nft_whale
union all
select * from smart_nft_trader
union all
select * from smart_nft_holder
union all
select * from smart_nft_sweeper
union all
select * from smart_nft_minter
union all
select * from smart_nft_golden_dog_minter
union all
select * from opensea_trader
union all
select * from legendary_nft_trader
union all
select * from epic_nft_trader
union all
select * from rare_nft_trader
union all
select * from uncommon_nft_trader
union all
select * from diversified_nft_holder
union all
select * from blue_chip_nft_holder
union all
select * from ens_name
union all
select * from ens_name_reverse
union all
select * from phish_hacker
