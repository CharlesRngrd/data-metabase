import duckdb
import pandas as pd


with duckdb.connect("assets/data.duckdb") as con:
    df_tour_1 = con.sql("SELECT * FROM election_2024_tour_1").fetchdf()
    df_tour_2 = con.sql("SELECT * FROM election_2024_tour_2").fetchdf()

    columns = {
        "Code département": "departement_code",
        "Libellé département": "departement_nom",
        "Code circonscription législative": "circonscription_code",
        "Libellé circonscription législative": "circonscription_nom",
        "% Abstentions": "abstention_pourcentage",
        "Exprimés": "exprime",
    }

    label_parti = {
        # Rassemblement National
        "RN": "Rassemblement National",
        "UDI": "Rassemblement National",
        "UXD": "Rassemblement National",
        "EXD": "Rassemblement National",
        # Les Républicains
        "LR": "Les Républicains",
        "DVD": "Les Républicains",
        # Nouveau Front Populaire
        "UG": "Nouveau Front Populaire",
        "SOC": "Nouveau Front Populaire",
        "ECO": "Nouveau Front Populaire",
        "DVG": "Nouveau Front Populaire",
        # Ensemble
        "ENS": "Ensemble",
        "HOR": "Ensemble",
        "DVC": "Ensemble",
        # Autre
        "REG": "Régionaliste",
        "DIV": "Divers",
    }

    def clean_pourcentage(serie):
        for old, new in ({"%": "", ",": "."}).items():
            serie = serie.str.replace(old, new)

        return pd.to_numeric(serie)

    def generate_columns_candidat(numero):
        return {
            f"Nuance candidat {numero}": "candidat_parti",
            f"Nom candidat {numero}": "candidat_nom",
            f"Prénom candidat {numero}": "candidat_prenom",
            f"Sexe candidat {numero}": "candidat_sexe",
            f"Voix {numero}": "candidat_voix",
            f"% Voix/exprimés {numero}": "candidat_voix_pourcentage",
        }

    def reshape_data(df, columns):
        df_final = pd.DataFrame()

        for numero in range(20):
            columns_tmp = columns.copy()
            columns_tmp.update(generate_columns_candidat(numero + 1))

            if f"Nom candidat {numero + 1}" not in df.columns:
                continue

            df_tmp = df.copy()
            df_tmp = df_tmp[columns_tmp.keys()]
            df_tmp = df_tmp.rename(columns=columns_tmp)

            df_final = pd.concat([df_final, df_tmp])

        df_final["candidat_denomination"] = (
            df_final["candidat_nom"] + " " + df_final["candidat_prenom"]
        )

        return df_final

    df_tour_1 = reshape_data(df_tour_1, columns)
    df_tour_2 = reshape_data(df_tour_2, columns)

    df_tour_2_complement = df_tour_1.loc[
        ~df_tour_1["circonscription_code"].isin(df_tour_2["circonscription_code"]), :
    ]

    df = pd.concat([df_tour_2, df_tour_2_complement])

    df["candidat_parti"] = df["candidat_parti"].replace(label_parti)

    df["departement_code"] = df["departement_code"].str.pad(
        width=2, side="left", fillchar="0"
    )
    df["circonscription_code"] = df["circonscription_code"].str.pad(
        width=4, side="left", fillchar="0"
    )

    df = df.sort_values(
        ["circonscription_code", "candidat_voix"], ascending=[True, False]
    )

    df["candidat_rang"] = df.groupby(["circonscription_code"])["candidat_voix"].rank(
        method="min", ascending=False
    )

    df = df[df["candidat_rang"].notna()]

    df["abstention_pourcentage"] = clean_pourcentage(df["abstention_pourcentage"])
    df["candidat_voix_pourcentage"] = clean_pourcentage(df["candidat_voix_pourcentage"])

    con.execute("CREATE OR REPLACE TABLE election_2024_mart AS SELECT * FROM df")
    con.table("election_2024_mart").show()
