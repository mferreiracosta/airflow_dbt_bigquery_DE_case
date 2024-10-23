WITH cte AS (
  SELECT DISTINCT
    regional,
    latitude,
    longitude,
    delegacia,
    uop

  FROM {{ ref('stg_acidentes_brasil') }}

  WHERE regional IS NOT NULL
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['regional', 'latitude', 'longitude', 'delegacia', 'uop']) }} AS id,
  cte.*

FROM cte
