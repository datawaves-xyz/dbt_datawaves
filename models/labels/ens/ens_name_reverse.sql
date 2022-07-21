  select 
    address,
    lower(ens_name) AS label,
    'ENS Name Reverse' as label_type
  from {{ ref("ens_ethereum_reverse_registrars") }}