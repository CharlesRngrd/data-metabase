import duckdb
import pandas as pd


with duckdb.connect("assets/data.duckdb") as con:
    df_pandas = con.sql("SELECT * FROM election_mart").fetchdf()
    print(df_pandas.shape)

    df_spark = con.sql("SELECT * FROM election_mart_spark").fetchdf()
    print(df_spark.shape)

    df_test = pd.concat([df_pandas, df_spark])
    print(df_test.shape)

    df_test = df_test.drop_duplicates(keep=False)
    print(df_test.shape)
