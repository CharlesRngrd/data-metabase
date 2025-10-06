import duckdb
import requests
import pandas as pd

# URL retrieved from :
# https://www.data.gouv.fr/datasets/elections-legislatives-des-30-juin-et-7-juillet-2024-resultats-definitifs-du-1er-tour/
# https://www.data.gouv.fr/datasets/elections-legislatives-des-30-juin-et-7-juillet-2024-resultats-definitifs-du-2nd-tour

tours = [
    "5163f2e3-1362-4c35-89a0-1934bb74f2d9",
    "1050324f-a0ce-4a06-94c5-e2f6805fdb74",
]


def get_data_election(label, tour):
    next_url = f"https://tabular-api.data.gouv.fr/api/resources/{tours[tour]}/data/?page_size=200"
    election_2024_list = []

    while next_url:
        election_2024_json = requests.get(next_url).json()

        next_url = election_2024_json["links"]["next"]

        election_2024_list.extend(election_2024_json["data"])

    df = pd.DataFrame(election_2024_list)

    print(f"Nombre de circonscriptions : {len(df)}")

    con = duckdb.connect("assets/data.duckdb")
    con.execute(f"CREATE TABLE IF NOT EXISTS {label} AS SELECT * FROM df")

    print(con.execute(f"SELECT * FROM {label} LIMIT 5").fetchdf())


get_data_election("election_2024_tour_1", 0)
get_data_election("election_2024_tour_2", 1)
