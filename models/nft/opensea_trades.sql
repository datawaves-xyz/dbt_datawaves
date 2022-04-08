WITH wyvern_data AS (
  SELECT * FROM {{ ref('wyvern_data') }}
)

SELECT * FROM wyvern_data
