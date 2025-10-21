from functools import reduce
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    length,
    lit,
    lpad,
    max,
    rank,
    regexp_replace,
    round,
    when,
)
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

label_parti = {
    # Rassemblement National
    "RN": "Rassemblement National",
    "UXD": "Rassemblement National",
    "EXD": "Rassemblement National",
    "DSV": "Rassemblement National",
    # Les Républicains
    "LR": "Les Républicains",
    "DVD": "Les Républicains",
    # Ensemble
    "ENS": "Ensemble",
    "HOR": "Ensemble",
    "UDI": "Ensemble",
    "DVC": "Ensemble",
    # Nouveau Front Populaire
    "NUP": "Nouveau Front Populaire",
    "UG": "Nouveau Front Populaire",
    "SOC": "Nouveau Front Populaire",
    "ECO": "Nouveau Front Populaire",
    "DVG": "Nouveau Front Populaire",
    "EXG": "Nouveau Front Populaire",
    "FI": "Nouveau Front Populaire",
    # Autre
    "REG": "Régionaliste",
    "DIV": "Divers",
}


def get_session() -> SparkSession:
    """Initialise la session Spark"""

    python = r"C:\\Users\\charl\\miniconda3\\envs\\pyspark-env\\python.exe"

    os.environ["PYSPARK_PYTHON"] = python
    os.environ["PYSPARK_DRIVER_PYTHON"] = python

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


def clean_common(df: DataFrame) -> DataFrame:
    """Nettoyage commun à tous les jeux de données"""

    df = df.withColumn(
        "candidat_denomination",
        concat_ws(" ", col("candidat_nom"), col("candidat_prenom")),
    )

    df = custom_padding(df, "departement_code", 2)
    df = custom_padding(df, "circonscription_code", 4)

    df = clean_pourcentage_column(df, "abstention_pourcentage")
    df = clean_pourcentage_column(df, "candidat_voix_pourcentage")

    return df


def clean_pourcentage_column(df: DataFrame, column_name: str) -> DataFrame:
    """Convertie une valeur du type '10,0%' en 10.0"""

    return df.withColumn(
        column_name,
        regexp_replace(
            regexp_replace(col(column_name).cast("string"), "%", ""), ",", "."
        ).cast("double"),
    )


def custom_padding(df: DataFrame, column_name: str, num: int) -> DataFrame:
    """Ajoute un padding en permettant des code plus long pour les DOM-TOM"""

    return df.withColumn(
        column_name,
        when(
            length(col(column_name)) < num, lpad(col(column_name), num, "0")
        ).otherwise(col(column_name)),
    )


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

    for column in to_remane_cols:
        df = df.withColumnRenamed(column, f"{column} 1")

    n_unnamed = len([c for c in df.columns if c.startswith("Unnamed")])
    n_groups = n_unnamed // len(to_remane_cols)

    new_cols = [c for c in df.columns if not c.startswith("Unnamed")]

    for i in range(2, n_groups + 2):
        new_cols += [f"{col} {i}" for col in to_remane_cols]

    return df.toDF(*new_cols)


def postprocess_2022_values(df: DataFrame) -> DataFrame:
    """Nettoyage spécifique aux datasets de 2022"""

    df = df.withColumn(
        "circonscription_code",
        concat(
            col("departement_code"),
            lpad(col("circonscription_code").cast("string"), 2, "0"),
        ),
    )

    return df


def merge_tour_1_2(df_tour_1: DataFrame, df_tour_2: DataFrame, year: int) -> DataFrame:
    """Concatenation du 1er et 2e tour"""

    df_extra = df_tour_1.join(
        df_tour_2.select("circonscription_code").distinct(),
        on="circonscription_code",
        how="left_anti",
    )

    df = df_tour_2.unionByName(df_extra)

    df = df.withColumn("annee", lit(year))

    return df


with duckdb.connect("assets/data.duckdb") as con:
    session = get_session()

    dfs: Dict[str, DataFrame] = dict()
    for df in ["2022_tour_1", "2022_tour_2", "2024_tour_1", "2024_tour_2"]:
        dfs[f"df_{df}"] = load_dataframes(df)

    dfs["df_2022_tour_1"] = dfs["df_2022_tour_1"].withColumn("Unnamed fake", lit(""))

    for df in ["df_2022_tour_1", "df_2022_tour_2"]:
        dfs[df] = preprocess_2022_format(dfs[df])

    for df in ["df_2022_tour_1", "df_2022_tour_2", "df_2024_tour_1", "df_2024_tour_2"]:
        dfs[df] = pivot_data(dfs[df], columns)

    for df in ["df_2022_tour_1", "df_2022_tour_2"]:
        dfs[df] = postprocess_2022_values(dfs[df])

    for df in ["df_2022_tour_1", "df_2022_tour_2", "df_2024_tour_1", "df_2024_tour_2"]:
        dfs[df] = clean_common(dfs[df])

    df_2022 = merge_tour_1_2(dfs["df_2022_tour_1"], dfs["df_2022_tour_2"], 2022)
    df_2024 = merge_tour_1_2(dfs["df_2024_tour_1"], dfs["df_2024_tour_2"], 2024)

    df = reduce(DataFrame.unionByName, [df_2022, df_2024])

    df = df.na.replace(label_parti, subset=["candidat_parti"])

    df = df.filter(col("candidat_voix_pourcentage").isNotNull())
    df = df.filter(col("candidat_voix_pourcentage") >= 25)

    df = df.sort(
        col("annee").asc(),
        col("circonscription_code").asc(),
        col("candidat_voix").desc(),
    )

    window = Window.partitionBy("annee", "circonscription_code").orderBy(
        col("candidat_voix_pourcentage").desc()
    )

    df = df.withColumn("candidat_rang", rank().over(window))

    df = df.withColumn(
        "candidat_rang_1_score", max("candidat_voix_pourcentage").over(window)
    )

    df = df.withColumn(
        "candidat_rang_1_ecart",
        round(col("candidat_voix_pourcentage") - col("candidat_rang_1_score"), 2),
    )

    con.register("df", df.toArrow())
    con.execute("CREATE OR REPLACE TABLE election_mart_spark AS SELECT * FROM df")
    con.table("election_mart_spark").show()
