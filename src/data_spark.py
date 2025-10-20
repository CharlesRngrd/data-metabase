from pyspark.sql import SparkSession
import os
import duckdb


venv_python = os.path.join(os.getcwd(), ".venv", "Scripts", "python.exe")

os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python
os.environ["HADOOP_HOME"] = "C:\\hadoop\\hadoop-3.0.0"

session = SparkSession.builder.appName("data-metabase").master("local[*]").getOrCreate()

print(f">>> {session.version}")

with duckdb.connect("assets/data.duckdb") as con:
    file = "assets/data.parquet"
    con.execute(f"COPY election_2024_tour_2 TO '{file}' (FORMAT PARQUET)")
    df_spark = session.read.parquet(file)
    df_spark.show(5)
