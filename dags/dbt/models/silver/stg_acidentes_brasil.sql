WITH source AS (
    SELECT *
    FROM {{ source('cobli', 'raw_acidentes_brasil') }}
),

transform AS (
    SELECT
        CAST(id AS STRING) AS id,
        PARSE_DATETIME('%d/%m/%Y %H:%M:%S', CONCAT(data_inversa, ' ', horario)) AS data_hora,
        TRIM(dia_semana) AS nome_dia_semana,
        TRIM(uf) AS uf,
        TRIM(municipio) AS cidade,
        TRIM(causa_acidente) AS causa_acidente,
        TRIM(tipo_acidente) AS tipo_acidente,
        TRIM(classificacao_acidente) AS classificacao_acidente,
        TRIM(fase_dia) AS fase_dia,
        TRIM(sentido_via) AS sentido_via,
        TRIM(condicao_metereologica) AS condicao_metereologica,
        TRIM(tipo_pista) AS tipo_pista,
        TRIM(tracado_via) AS tracado_via,
        CASE WHEN uso_solo = 'Sim' THEN 1 ELSE 0 END AS uso_solo,
        br,
        km,
        CAST(pessoas AS INT) AS pessoas,
        CAST(mortos AS INT) AS mortos,
        CAST(feridos_leves AS INT) AS feridos_leves,
        CAST(feridos_graves AS INT) AS feridos_graves,
        CAST(ilesos AS INT) AS ilesos,
        CAST(ignorados AS INT) AS ignorados,
        CAST(feridos AS INT) AS feridos,
        CAST(veiculos AS INT) AS veiculos,
        TRIM(latitude) AS latitude,
        TRIM(longitude) AS longitude,
        TRIM(regional) AS regional,
        TRIM(delegacia) AS delegacia,
        TRIM(uop) AS uop

    FROM source
)

SELECT *
FROM transform
