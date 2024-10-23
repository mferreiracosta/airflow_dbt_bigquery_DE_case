WITH cte AS (
  SELECT DISTINCT
    cidade,
    uf

  FROM {{ ref('stg_acidentes_brasil') }}

  WHERE cidade IS NOT NULL
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['cidade', 'uf']) }} AS id,
  cte.*

FROM cte
