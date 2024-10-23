WITH fct_acidentes_brasil_cte AS (
    SELECT
        id,
        {{ dbt_utils.generate_surrogate_key(['CAST(data_hora AS STRING)']) }} AS datahora_id,
        {{ dbt_utils.generate_surrogate_key(['cidade', 'uf']) }} AS cidade_id,
        {{ dbt_utils.generate_surrogate_key(['regional', 'latitude', 'longitude', 'delegacia', 'uop']) }} AS regional_id,
        {{ dbt_utils.generate_surrogate_key(['causa_acidente', 'tipo_acidente', 'classificacao_acidente', 'fase_dia', 'sentido_via', 'condicao_metereologica', 'tipo_pista', 'tracado_via', 'uso_solo']) }} AS acidente_id,
        br,
        km,
        pessoas,
        mortos,
        feridos_leves,
        feridos_graves,
        ilesos,
        ignorados,
        feridos,
        veiculos

    FROM {{ ref('stg_acidentes_brasil') }}
)
SELECT
    fct.id,
    dt.id AS data_hora_id,
    dc.id AS cidade_id,
    dr.id AS regional_id,
    da.id AS acidente_id,
    fct.br,
    fct.km,
    fct.pessoas,
    fct.mortos,
    fct.feridos_leves,
    fct.feridos_graves,
    fct.ilesos,
    fct.ignorados,
    fct.feridos,
    fct.veiculos

FROM fct_acidentes_brasil_cte AS fct
INNER JOIN {{ ref('dim_tempo') }} AS dt ON fct.datahora_id = dt.id
INNER JOIN {{ ref('dim_cidade') }} AS dc ON fct.cidade_id = dc.id
INNER JOIN {{ ref('dim_regional') }} AS dr ON fct.regional_id = dr.id
INNER JOIN {{ ref('dim_acidente') }} AS da ON fct.acidente_id = da.id
