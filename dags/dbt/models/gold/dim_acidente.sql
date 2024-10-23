WITH cte AS (
  SELECT DISTINCT
    causa_acidente,
    tipo_acidente,
    classificacao_acidente,
    fase_dia,
    sentido_via,
    condicao_metereologica,
    tipo_pista,
    tracado_via,
    uso_solo

  FROM {{ ref('stg_acidentes_brasil') }}

  WHERE causa_acidente IS NOT NULL
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['causa_acidente', 'tipo_acidente', 'classificacao_acidente', 'fase_dia', 'sentido_via', 'condicao_metereologica', 'tipo_pista', 'tracado_via', 'uso_solo']) }} AS id,
  cte.*

FROM cte
