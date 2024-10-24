import dask.dataframe as dd


def transform(filename: str, input_path: str, output_path: str):
    # Define 'source' and 'destination' path
    source = f"{input_path}/{filename}"
    destination = f"{output_path}/{filename}"

    # Read parquet file using dask
    df = dd.read_parquet(source, engine="pyarrow")

    # Adjust 'id' data_type to string
    df['id'] = df['id'].astype(str)

    # Combine 'data_inversa' and 'horario' columns, and parse to datetime type
    df['data_hora'] = dd.to_datetime(df['data_inversa'] + ' ' + df['horario'], format='%d/%m/%Y %H:%M:%S')

    # Applying trim and renamed 'dia_semana' column to 'nome_dia_semana'
    df['nome_dia_semana'] = df['dia_semana'].str.strip()

    # Applying trim and changing data_type to string
    string_cols = ['uf', 'municipio', 'causa_acidente', 'tipo_acidente',
                   'classificacao_acidente', 'fase_dia', 'sentido_via', 'condicao_metereologica',
                   'tipo_pista', 'tracado_via', 'latitude', 'longitude', 'regional', 'delegacia', 'uop']

    for col in string_cols:
        df[col] = df[col].str.strip()

    # Convert 'uso_solo' column to 1 or 0
    df['uso_solo'] = df['uso_solo'].apply(lambda x: 1 if x == 'Sim' else (0 if x == 'Nao' else None), meta=('uso_solo', 'int64'))
    
    # Convert numeric_cols to int
    numeric_cols = ['pessoas', 'mortos', 'feridos_leves', 'feridos_graves', 'ilesos', 'ignorados', 'feridos', 'veiculos']
    for col in numeric_cols:
        df[col] = df[col].astype('int64')

    # Load dataframe into parquet using pyarrow engine, snappy compression and
    # partition parquet file by 'ano' and 'uf' columns
    df.to_parquet(
        destination,
        engine="pyarrow",
        write_metadata_file=False,
        compression="snappy",
        name_function=lambda n: f"part-{n}.snappy.parquet",
        partition_on=["ano", "uf"],
        overwrite=True,
    )
    print(f"Arquivo CSV {source} convertido e salvo como Parquet em {destination}")


if __name__ == "__main__":
    filename = "acidentes_brasil"
    input_path = "include/datalake/bronze"
    output_path = "include/datalake/silver"

    transform(filename, input_path, output_path)
