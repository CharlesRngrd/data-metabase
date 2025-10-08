import duckdb
import pandas as pd
from pandas.api.types import is_numeric_dtype


with duckdb.connect("assets/data.duckdb") as con:
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

    label_parti = {
        # Rassemblement National
        "RN": "Rassemblement National",
        "UXD": "Rassemblement National",
        "EXD": "Rassemblement National",
        # Les Républicains
        "LR": "Les Républicains",
        "DVD": "Les Républicains",
        # Ensemble
        "ENS": "Ensemble",
        "HOR": "Ensemble",
        "UDI": "Ensemble",
        "DVC": "Ensemble",
        # Nouveau Front Populaire
        "UG": "Nouveau Front Populaire",
        "SOC": "Nouveau Front Populaire",
        "ECO": "Nouveau Front Populaire",
        "DVG": "Nouveau Front Populaire",
        # Autre
        "REG": "Régionaliste",
        "DIV": "Divers",
    }

    def generate_columns_candidat(numero):
        return {
            # Format 2022
            f"Nuance {numero}": "candidat_parti",
            f"Nom {numero}": "candidat_nom",
            f"Prénom {numero}": "candidat_prenom",
            f"Sexe {numero}": "candidat_sexe",
            f"Voix {numero}": "candidat_voix",
            f"% Voix/Exp {numero}": "candidat_voix_pourcentage",
            # Format 2024
            f"Nuance candidat {numero}": "candidat_parti",
            f"Nom candidat {numero}": "candidat_nom",
            f"Prénom candidat {numero}": "candidat_prenom",
            f"Sexe candidat {numero}": "candidat_sexe",
            f"% Voix/exprimés {numero}": "candidat_voix_pourcentage",
        }

    def load_dataframes():
        elections = ["2022_tour_1", "2022_tour_2", "2024_tour_1", "2024_tour_2"]

        return {
            f"df_{election}": con.sql(f"SELECT * FROM election_{election}").fetchdf()
            for election in elections
        }

    def pivot_data(df, columns):
        df_final = pd.DataFrame()

        for numero in range(100):
            columns_tmp = columns.copy()
            columns_tmp.update(generate_columns_candidat(numero + 1))

            if not any(col for col in df.columns if col.endswith(str(numero + 1))):
                continue

            df_tmp = df.copy()
            df_tmp = df_tmp[df_tmp.columns.intersection(columns_tmp.keys())]
            df_tmp = df_tmp.rename(columns=columns_tmp)

            df_final = pd.concat([df_final, df_tmp])
        
        return df_final

    def clean_common(df):
        df["candidat_denomination"] = (
            df["candidat_nom"] + " " + df["candidat_prenom"]
        )

        df["departement_code"] = df["departement_code"].str.pad(
            width=2, side="left", fillchar="0"
        )

        df["circonscription_code"] = df["circonscription_code"].str.pad(
            width=4, side="left", fillchar="0"
        )

        return df

    def clean_pourcentage(serie):
        for old, new in ({"%": "", ",": "."}).items():
            serie = serie.astype(str).str.replace(old, new)

        return pd.to_numeric(serie)

    def preprocess_2022_format(df):
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

    def clean_2022_values(df):
        df["circonscription_code"] = df["departement_code"] + df["circonscription_code"].astype(str).str.pad(
            width=2, side="left", fillchar="0"
        )

        return df

    def merge_tours(df_tour_1, df_tour_2, year):
        df_tour_2_complement = df_tour_1.loc[
            ~df_tour_1["circonscription_code"].isin(df_tour_2["circonscription_code"]), :
        ]

        df = pd.concat([df_tour_2, df_tour_2_complement])

        df["annee"] = year

        return df

    dfs = load_dataframes()

    dfs["df_2022_tour_1"]["Unnamed fake"] = ""

    dfs["df_2022_tour_1"] = preprocess_2022_format(dfs["df_2022_tour_1"])
    dfs["df_2022_tour_2"] = preprocess_2022_format(dfs["df_2022_tour_2"])

    dfs["df_2022_tour_1"] = pivot_data(dfs["df_2022_tour_1"], columns)
    dfs["df_2022_tour_2"] = pivot_data(dfs["df_2022_tour_2"], columns)
    dfs["df_2024_tour_1"] = pivot_data(dfs["df_2024_tour_1"], columns)
    dfs["df_2024_tour_2"] = pivot_data(dfs["df_2024_tour_2"], columns)

    dfs["df_2022_tour_1"] = clean_2022_values(dfs["df_2022_tour_1"])
    dfs["df_2022_tour_2"] = clean_2022_values(dfs["df_2022_tour_2"])

    dfs["df_2022_tour_1"] = clean_common(dfs["df_2022_tour_1"])
    dfs["df_2022_tour_2"] = clean_common(dfs["df_2022_tour_2"])
    dfs["df_2024_tour_1"] = clean_common(dfs["df_2024_tour_1"])
    dfs["df_2024_tour_2"] = clean_common(dfs["df_2024_tour_2"])

    df_2022 = merge_tours(dfs["df_2022_tour_1"], dfs["df_2022_tour_2"], 2022)
    df_2024 = merge_tours(dfs["df_2024_tour_1"], dfs["df_2024_tour_2"], 2024)

    df = pd.concat([df_2022, df_2024])

    df["candidat_parti"] = df["candidat_parti"].replace(label_parti)

    df = df.sort_values(
        ["annee", "circonscription_code", "candidat_voix"], ascending=[True, True, False]
    )

    df["candidat_rang"] = df.groupby(["annee", "circonscription_code"])["candidat_voix"].rank(
        method="min", ascending=False
    )

    df = df[df["candidat_rang"].notna()]

    df["abstention_pourcentage"] = clean_pourcentage(df["abstention_pourcentage"])
    df["candidat_voix_pourcentage"] = clean_pourcentage(df["candidat_voix_pourcentage"])

    print(df[df["annee"] == 2022]["candidat_voix_pourcentage"])

    con.execute("CREATE OR REPLACE TABLE election_mart AS SELECT * FROM df")
    con.table("election_mart").show()
