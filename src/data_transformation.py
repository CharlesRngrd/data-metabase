import duckdb
import pandas as pd
from typing import Dict


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


def load_dataframes(df_name: str) -> Dict[str, pd.DataFrame]:
    """Charge une table depuis DuckDB"""

    return con.sql(f"SELECT * FROM election_{df_name}").fetchdf()


def pivot_data(df: pd.DataFrame, columns: Dict[str, str]) -> pd.DataFrame:
    """Transforme un dataframe électoral large en format long par candidat"""

    df_final = pd.DataFrame()

    for numero in range(100):
        columns_tmp = columns.copy()

        columns_tmp.update(
            {f"{key} {numero + 1}": value for key, value in columns_dynamic.items()}
        )

        if not any(col.endswith(str(numero + 1)) for col in df.columns):
            continue

        df_tmp = df.copy()
        df_tmp = df_tmp[df.columns.intersection(columns_tmp.keys())]
        df_tmp = df_tmp.rename(columns=columns_tmp)

        df_final = pd.concat([df_final, df_tmp])

    return df_final


def clean_common(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage commun à tous les jeux de données"""

    df["candidat_denomination"] = df["candidat_nom"] + " " + df["candidat_prenom"]

    df["departement_code"] = df["departement_code"].str.zfill(2)
    df["circonscription_code"] = df["circonscription_code"].str.zfill(4)

    return df


def clean_pourcentage(serie: pd.Series) -> pd.Series:
    """Convertie une valeur du type '10,0%' en 10.0"""

    for old, new in ({"%": "", ",": "."}).items():
        serie = serie.astype(str).str.replace(old, new)

    return pd.to_numeric(serie)


def preprocess_2022_format(df: pd.DataFrame) -> pd.DataFrame:
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
        df = df.rename(columns={col: f"{col} 1"})

    n_unnamed = len([c for c in df.columns if c.startswith("Unnamed")])
    n_groups = n_unnamed // len(to_remane_cols)

    new_cols = [c for c in df.columns if not c.startswith("Unnamed")]
    for i in range(2, n_groups + 2):
        new_cols += [f"{col} {i}" for col in to_remane_cols]

    df.columns = new_cols

    return df


def postprocess_2022_values(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage spécifique aux datasets de 2022"""

    df["circonscription_code"] = df["circonscription_code"].astype(str).str.zfill(2)
    df["circonscription_code"] = df["departement_code"] + df["circonscription_code"]

    return df


def merge_tour_1_2(
    df_tour_1: pd.DataFrame, df_tour_2: pd.DataFrame, year: int
) -> pd.DataFrame:
    """Concatenation du 1er et 2e tour"""

    df_extra = df_tour_1.loc[
        ~df_tour_1["circonscription_code"].isin(df_tour_2["circonscription_code"]),
        :,
    ]

    df = pd.concat([df_tour_2, df_extra])

    df["annee"] = year

    return df


with duckdb.connect("assets/data.duckdb") as con:
    dfs: Dict[str, pd.DataFrame] = dict()
    for df in ["2022_tour_1", "2022_tour_2", "2024_tour_1", "2024_tour_2"]:
        dfs[f"df_{df}"] = load_dataframes(df)

    dfs["df_2022_tour_1"]["Unnamed fake"] = ""

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

    df = pd.concat([df_2022, df_2024])

    df["candidat_parti"] = df["candidat_parti"].replace(label_parti)

    df = df[df["candidat_voix_pourcentage"].notna()]

    df["abstention_pourcentage"] = clean_pourcentage(df["abstention_pourcentage"])
    df["candidat_voix_pourcentage"] = clean_pourcentage(df["candidat_voix_pourcentage"])

    df = df[df["candidat_voix_pourcentage"] >= 25]

    df = df.sort_values(
        ["annee", "circonscription_code", "candidat_voix"],
        ascending=[True, True, False],
    )

    grouped = df.groupby(["annee", "circonscription_code"])["candidat_voix_pourcentage"]

    df["candidat_rang"] = grouped.rank(method="min", ascending=False)
    df["candidat_rang_1_score"] = grouped.transform("max")

    df["candidat_rang_1_ecart"] = (
        df["candidat_voix_pourcentage"] - df["candidat_rang_1_score"]
    )

    df = df[df["candidat_voix_pourcentage"] >= 25]

    con.execute("CREATE OR REPLACE TABLE election_mart AS SELECT * FROM df")
    con.table("election_mart").show()
