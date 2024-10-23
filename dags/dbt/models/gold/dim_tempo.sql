WITH cte AS (
  SELECT DISTINCT
    CAST(data_hora AS STRING) AS data_hora_str,
    data_hora,
    nome_dia_semana

  FROM {{ ref('stg_acidentes_brasil') }}

  WHERE data_hora IS NOT NULL
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['data_hora_str']) }} AS id,
  data_hora,
  EXTRACT(DAY FROM data_hora) AS dia,
  EXTRACT(MONTH FROM data_hora) AS mes,
  EXTRACT(YEAR FROM data_hora) AS ano,
  EXTRACT(HOUR FROM data_hora) AS hora,
  EXTRACT(MINUTE FROM data_hora) AS minuto,
  EXTRACT(DAYOFWEEK FROM data_hora) AS dia_semana,
  nome_dia_semana

FROM cte
