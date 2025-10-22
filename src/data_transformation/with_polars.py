import time
from typing import Dict

import duckdb
import polars as pl


start_time = time.time()

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


def load_dataframes(df_name: str) -> Dict[str, pl.DataFrame]:
    """Charge une table depuis DuckDB"""

    return con.sql(f"SELECT * FROM election_{df_name}").pl()


def pivot_data(df: pl.DataFrame, columns: Dict[str, str]) -> pl.DataFrame:
    """Transforme un dataframe électoral large en format long par candidat"""

    df_final = pl.DataFrame()

    for numero in range(100):
        columns_tmp = columns.copy()

        columns_tmp.update(
            {f"{key} {numero + 1}": value for key, value in columns_dynamic.items()}
        )

        if not any(col.endswith(str(numero + 1)) for col in df.columns):
            continue

        df_tmp = df.select([col for col in df.columns if col in columns_tmp.keys()])
        df_tmp = df_tmp.rename(columns_tmp, strict=False)

        df_tmp = df_tmp.with_columns(pl.col("candidat_voix").cast(pl.Int64))

        df_final = pl.concat([df_final, df_tmp])

    return df_final


def clean_common(df: pl.DataFrame) -> pl.DataFrame:
    """Nettoyage commun à tous les jeux de données"""

    df = df.with_columns(
        (pl.col("candidat_nom") + " " + pl.col("candidat_prenom")).alias(
            "candidat_denomination"
        )
    )

    df = df.with_columns(
        pl.col("departement_code").cast(pl.Utf8).str.zfill(2).alias("departement_code")
    )

    df = df.with_columns(
        pl.col("circonscription_code")
        .cast(pl.Utf8)
        .str.zfill(4)
        .alias("circonscription_code")
    )

    df = clean_pourcentage(df, "abstention_pourcentage")
    df = clean_pourcentage(df, "candidat_voix_pourcentage")

    return df


def clean_pourcentage(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    """Convertie une valeur du type '10,0%' en 10.0"""

    return df.with_columns(
        pl.col(column_name)
        .cast(pl.Utf8)
        .str.replace_all("%", "")
        .str.replace_all(",", ".")
        .cast(pl.Float64)
    )


def preprocess_2022_format(df: pl.DataFrame) -> pl.DataFrame:
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

    df = df.rename({col: f"{col} 1" for col in to_remane_cols})

    n_unnamed = len([c for c in df.columns if c.startswith("Unnamed")])
    n_groups = n_unnamed // len(to_remane_cols)

    new_cols = [c for c in df.columns if not c.startswith("Unnamed")]
    for i in range(2, n_groups + 2):
        new_cols += [f"{col} {i}" for col in to_remane_cols]

    df.columns = new_cols

    return df


def postprocess_2022_values(df: pl.DataFrame) -> pl.DataFrame:
    """Nettoyage spécifique aux datasets de 2022"""

    df = df.with_columns(
        [
            pl.col("circonscription_code")
            .cast(pl.Utf8)
            .str.zfill(2)
            .alias("circonscription_code"),
        ]
    )

    df = df.with_columns(
        (pl.col("departement_code") + pl.col("circonscription_code")).alias(
            "circonscription_code"
        )
    )

    return df


def merge_tour_1_2(
    df_tour_1: pl.DataFrame, df_tour_2: pl.DataFrame, year: int
) -> pl.DataFrame:
    """Concatenation du 1er et 2e tour"""

    df_extra = df_tour_1.filter(
        ~pl.col("circonscription_code").is_in(df_tour_2["circonscription_code"].to_list())
    )

    df = pl.concat([df_tour_2, df_extra])

    df = df.with_columns(pl.lit(year).alias("annee"))

    return df


with duckdb.connect("assets/data.duckdb") as con:
    dfs: Dict[str, pl.DataFrame] = dict()
    for df in ["2022_tour_1", "2022_tour_2", "2024_tour_1", "2024_tour_2"]:
        dfs[f"df_{df}"] = load_dataframes(df)

    dfs["df_2022_tour_1"] = dfs["df_2022_tour_1"].with_columns(
        pl.lit("").alias("Unnamed fake")
    )

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

    # Need this technical reshape because columns are not in the same order.
    df_2024 = df_2024.select([c for c in df_2022.columns if c in df_2022.columns])

    df = pl.concat([df_2022, df_2024])

    df = df.with_columns(
        pl.col("candidat_parti").replace(label_parti).alias("candidat_parti")
    )

    df = df.filter(pl.col("candidat_voix_pourcentage").is_not_nan())
    df = df.filter(pl.col("candidat_voix_pourcentage") >= 25)

    df = df.sort(
        by=["annee", "circonscription_code", "candidat_voix"],
        descending=[False, False, True],
    )

    df = df.with_columns(
        pl.col("candidat_voix_pourcentage")
        .rank(method="min", descending=True)
        .over("annee", "circonscription_code")
        .alias("candidat_rang"),
        pl.col("candidat_voix_pourcentage")
        .max()
        .over("annee", "circonscription_code")
        .alias("candidat_rang_1_score"),
    )

    df = df.with_columns(
        (pl.col("candidat_voix_pourcentage") - pl.col("candidat_rang_1_score"))
        .round(2)
        .alias("candidat_rang_1_ecart")
    )

    con.execute("CREATE OR REPLACE TABLE election_mart_polars AS SELECT * FROM df")
    con.table("election_mart_polars").show()

end_time = time.time()

print(end_time - start_time)
