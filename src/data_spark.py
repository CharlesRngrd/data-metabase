from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import Dict
import duckdb
import os


columns = {
    # Format 2022
    "Code du département": "departement_code",
    "Libellé du département": "departement_nom",
    "Code de la circonscription": "circonscription_code",
    "Libellé de la circonscription": "circonscription_nom",
    "% Abs/Ins": "abstention_pourcentage",
    "Exprimés": "exprime",
    # Format 2024
    "Code département": "departement_code",
    "Libellé département": "departement_nom",
    "Code circonscription législative": "circonscription_code",
    "Libellé circonscription législative": "circonscription_nom",
    "% Abstentions": "abstention_pourcentage",
}

columns_dynamic = {
    # Format 2022
    "Nuance": "candidat_parti",
    "Nom": "candidat_nom",
    "Prénom": "candidat_prenom",
    "Sexe": "candidat_sexe",
    "Voix": "candidat_voix",
    "% Voix/Exp": "candidat_voix_pourcentage",
    # Format 2024
    "Nuance candidat": "candidat_parti",
    "Nom candidat": "candidat_nom",
    "Prénom candidat": "candidat_prenom",
    "Sexe candidat": "candidat_sexe",
    "% Voix/exprimés": "candidat_voix_pourcentage",
}


def get_session() -> SparkSession:
    """Initialise la session Spark"""

    venv_python = os.path.join(os.getcwd(), ".venv", "Scripts", "python.exe")

    os.environ["PYSPARK_PYTHON"] = venv_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python
    os.environ["HADOOP_HOME"] = "C:\\hadoop\\hadoop-3.0.0"

    session = (
        SparkSession.builder.appName("data-metabase").master("local[*]").getOrCreate()
    )

    print(f">>> {session.version}")

    return session


def load_dataframes(df_name: str) -> Dict[str, DataFrame]:
    """Charge une table depuis DuckDB"""

    file_name = f"assets/election_{df_name}.parquet"
    con.execute(f"COPY election_{df_name} TO '{file_name}' (FORMAT PARQUET)")

    return session.read.parquet(file_name)


def pivot_data(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    """Transforme un dataframe électoral large en format long par candidat"""

    dfs = list()

    for numero in range(100):
        columns_tmp = columns.copy()

        columns_tmp.update(
            {f"{key} {numero + 1}": value for key, value in columns_dynamic.items()}
        )

        if not any(col.endswith(str(numero + 1)) for col in df.columns):
            continue

        df_tmp = df.rdd.map(lambda x: x).toDF(schema=df.schema)
        df_tmp = df_tmp.select(
            list(set(df.columns).intersection(set(columns_tmp.keys())))
        )
        df_tmp = df_tmp.withColumnsRenamed(columns_tmp)

        dfs.append(df_tmp)

    return reduce(DataFrame.unionByName, dfs)


def preprocess_2022_format(df: DataFrame) -> DataFrame:
    """Nettoyage spécifique aux datasets de 2022"""

    to_remane_cols = [
        "N°Panneau",
        "Sexe",
        "Nom",
        "Prénom",
        "Nuance",
        "Voix",
        "% Voix/Ins",
        "% Voix/Exp",
        "Sièges",
    ]

    for col in to_remane_cols:
        df = df.withColumnRenamed(col, f"{col} 1")

    n_unnamed = len([c for c in df.columns if c.startswith("Unnamed")])
    n_groups = n_unnamed // len(to_remane_cols)

    new_cols = [c for c in df.columns if not c.startswith("Unnamed")]

    for i in range(2, n_groups + 2):
        new_cols += [f"{col} {i}" for col in to_remane_cols]

    return df.toDF(*new_cols)


with duckdb.connect("assets/data.duckdb") as con:
    session = get_session()

    dfs: Dict[str, DataFrame] = dict()
    for df in ["2022_tour_1", "2022_tour_2", "2024_tour_1", "2024_tour_2"]:
        dfs[f"df_{df}"] = load_dataframes(df)

    dfs["df_2022_tour_1"] = dfs["df_2022_tour_1"].withColumn("Unnamed fake", lit(""))

    for df in ["df_2022_tour_1", "df_2022_tour_2"]:
        dfs[df] = preprocess_2022_format(dfs[df])

    for df in ["df_2022_tour_1"]:
        dfs[df] = pivot_data(dfs[df], columns)

    dfs["df_2022_tour_1"].show(5)
