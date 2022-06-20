with info (
  select *
  from {{ source("ethereum_property_materials", "phishing") }}
)

select
  address,
  'phishing' as label,
  'Phish Hacker' as label_type
from info