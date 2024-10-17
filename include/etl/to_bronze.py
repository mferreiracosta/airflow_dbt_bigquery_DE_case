import dask.dataframe as dd


def convert_csv_to_bronze_parquet(table_name: str, input_path: str, output_path: str, input_format: str = "csv"):
  
    # Define 'source' and 'destination' path
    source = f"{input_path}/{table_name}.{input_format}"
    destination = f"{output_path}/{table_name}"

    # Read csv file using dask
    df = dd.read_csv(
        source,
        sep=";",
        low_memory=False,
        dtype="object"
    )

    # Transform 'data_inversa' column to datetime type as 'dd/MM/yyyy'
    df['data_inversa'] = dd.to_datetime(df['data_inversa'], format='%d/%m/%Y')
    df['ano'] = df['data_inversa'].dt.year

    # Load dataframe into parquet using pyarrow engine, snappy compression and
    # partition parquet file by 'ano' and 'uf' columns
    df.to_parquet(
        destination,
        engine="pyarrow",
        write_metadata_file=True,
        compression="snappy",
        name_function=lambda n: f"part-{n}.snappy.parquet",
        partition_on=["ano", "uf"]
    )
