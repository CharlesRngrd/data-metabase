import duckdb
import requests
import pandas as pd

# URL retrieved from :
# https://www.data.gouv.fr/datasets/elections-legislatives-des-12-et-19-juin-2022-resultats-definitifs-du-premier-tour/
# https://www.data.gouv.fr/datasets/elections-legislatives-des-12-et-19-juin-2022-resultats-definitifs-du-second-tour/
# https://www.data.gouv.fr/datasets/elections-legislatives-des-30-juin-et-7-juillet-2024-resultats-definitifs-du-1er-tour/
# https://www.data.gouv.fr/datasets/elections-legislatives-des-30-juin-et-7-juillet-2024-resultats-definitifs-du-2nd-tour

elections = {
    "election_2022_tour_1": "a5e0199c-c98b-43b5-8a9f-e8a8b4133b2e",
    "election_2022_tour_2": "3ce8fa1a-a0d3-4012-b9c0-10c480cefeda",
    "election_2024_tour_1": "5163f2e3-1362-4c35-89a0-1934bb74f2d9",
    "election_2024_tour_2": "1050324f-a0ce-4a06-94c5-e2f6805fdb74",
}


def get_data_election(label, election_id):
    next_url = f"https://tabular-api.data.gouv.fr/api/resources/{election_id}/data/?page_size=200"
    election_list = []

    while next_url:
        response = requests.get(next_url)
        response.raise_for_status()

        next_url = response.json()["links"]["next"]
        election_list.extend(response.json()["data"])

    with duckdb.connect("assets/data.duckdb") as con:
        con.register("df", pd.DataFrame(election_list))
        con.execute(f"CREATE OR REPLACE TABLE {label} AS SELECT * FROM df")
        con.table(label).show()


for dataset, election_id in elections.items():
    get_data_election(dataset, election_id)
