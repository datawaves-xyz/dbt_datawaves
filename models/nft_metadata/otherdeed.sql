with lands as (
  select *
  from {{ source('ethereum_nft_metadata', 'otherdeed_lands') }}
),

kodas as (
  select *
  from {{ source('ethereum_nft_metadata', 'otherdeed_kodas') }}
),

combined as (
  select
    lands.token_id as token_id,
    lands.image,
    lands.category,
    lands.sediment,
    lands.environment,
    lands.artifact,
    lands.eastern,
    lands.southern,
    lands.western,
    lands.northern,
    kodas.token_id as koda_id,
    kodas.image as koda_image,
    kodas.head as koda_head,
    kodas.eyes as koda_eyes,
    kodas.core as koda_core,
    kodas.clothing as koda_clothing,
    kodas.weapon as koda_weapon,
    int(lands.environment_tier) as environment_tier,
    int(lands.sediment_tier) as sediment_tier,
    int(lands.eastern_tier) as eastern_tier,
    int(lands.southern_tier) as southern_tier,
    int(lands.western_tier) as western_tier,
    int(lands.northern_tier) as northern_tier,
    int(lands.plot) as plot,
    coalesce(lands.artifact is not null, false) as has_artifact,
    coalesce(lands.koda is not null, false) as has_koda,
    coalesce(int(kodas.token_id) >= 9901 and int(kodas.token_id) <= 9999, false) as koda_is_mega,
    coalesce(kodas.clothing is not null, false) as koda_has_clothing,
    coalesce(kodas.weapon is not null, false) as koda_has_weapon
  from lands

  left join kodas
    on lands.koda = kodas.token_id
)

select *
from combined
