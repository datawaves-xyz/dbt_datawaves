  select 
    owner AS address,
    lower(name) AS label,
    'ENS Name' as label_type
  from {{ ref("ens_ethereum_registrations") }}