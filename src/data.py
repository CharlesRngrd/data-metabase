import duckdb
import requests
import pandas as pd

# URL retrieved from :
# https://www.data.gouv.fr/datasets/elections-legislatives-des-30-juin-et-7-juillet-2024-resultats-definitifs-du-2nd-tour

next_url = "https://tabular-api.data.gouv.fr/api/resources/1050324f-a0ce-4a06-94c5-e2f6805fdb74/data/?page_size=200"
election_2024_list = []

while next_url:
    election_2024_json = requests.get(next_url).json()

    next_url = election_2024_json["links"]["next"]

    election_2024_list.extend(election_2024_json["data"])

df = pd.DataFrame(election_2024_list)

print(f"Nombre de circonscriptions : {len(df)}")

con = duckdb.connect("assets/data.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS election_2024_json AS SELECT * FROM df")

print(con.execute("SELECT * FROM election_2024_json LIMIT 5").fetchdf())
