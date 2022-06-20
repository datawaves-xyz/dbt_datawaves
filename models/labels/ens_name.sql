with name_registered_1 as (
  select *
  from {{ source('ens','ens_ethregistrarcontroller_evt_nameregistered') }}
),

name_registered_2 as (
  select *
  from {{ source('ens','ens_ethregistrarcontroller2_evt_nameregistered') }}
),

name_registered_3 as (
  select *
  from {{ source('ens','ens_ethregistrarcontroller3_evt_nameregistered') }}
),

ens_label_1 as (
  select
    owner AS address,
    lower(name) AS label,
    'ENS Name' as label_type
  from name_registered_1
),

ens_label_2 as (
  select
    owner AS address,
    lower(name) AS label,
    'ENS Name' as label_type
  from name_registered_2
),

ens_label_3 as (
  select
    owner AS address,
    lower(name) AS label,
    'ENS Name' as label_type
  from name_registered_3
)

select * from ens_label_1
union
select * from ens_label_2
union
select * from ens_label_3
