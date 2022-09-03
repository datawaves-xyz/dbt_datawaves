{{config(alias='registrations')}}

select
  *
from {{ ref('ens_ethereum_registrations') }}