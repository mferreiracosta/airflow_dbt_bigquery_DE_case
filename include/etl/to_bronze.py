import dask.dataframe as dd


def convert_csv_to_parquet_local_bronze(filename: str, input_path: str, output_path: str, input_format: str = "csv"):
  
    # Define 'source' and 'destination' path
    source = f"{input_path}/{filename}/*.{input_format}"
    destination = f"{output_path}/{filename}"

    # Read csv file using dask
    df = dd.read_csv(
        source,
        sep=";",
        low_memory=False,
        dtype="object"
    )

    # Transform 'data_inversa' column to datetime type as 'dd/MM/yyyy'
    df['ano'] = dd.to_datetime(df['data_inversa'], format='%d/%m/%Y').dt.year

    # Load dataframe into parquet using pyarrow engine, snappy compression and
    # partition parquet file by 'ano' and 'uf' columns
    df.to_parquet(
        destination,
        engine="pyarrow",
        write_metadata_file=True,
        compression="snappy",
        name_function=lambda n: f"part-{n}.snappy.parquet",
        partition_on=["ano", "uf"],
        overwrite=True,
    )
    print(f"Arquivo CSV {source} convertido e salvo como Parquet em {destination}")


def convert_csv_to_parquet_gcs_bronze(filename: str, input_path: str, output_path: str, input_format: str = "csv"):
    source = f"{input_path}/{filename}/*.{input_format}"
    destination = f"{output_path}/{filename}"

    # Read csv file using dask
    df = dd.read_csv(
        source,
        sep=";",
        low_memory=False,
        dtype="object"
    )

    # Transform 'data_inversa' column to datetime type as 'dd/MM/yyyy'
    df['ano'] = dd.to_datetime(df['data_inversa'], format='%d/%m/%Y').dt.year

    # Load dataframe into parquet using pyarrow engine, snappy compression and
    # partition parquet file by 'ano' and 'uf' columns
    df.to_parquet(
        destination,
        engine="pyarrow",
        write_metadata_file=False,
        compression="snappy",
        name_function=lambda n: f"part-{n}.snappy.parquet",
        overwrite=True,
    )
    print(f"Arquivo CSV {source} convertido e salvo como Parquet em {destination}")
