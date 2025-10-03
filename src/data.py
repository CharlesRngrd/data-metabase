import duckdb
import requests
import pandas as pd

# URL retrieved from :
# https://www.data.gouv.fr/datasets/elections-legislatives-des-30-juin-et-7-juillet-2024-resultats-definitifs-du-2nd-tour
election_2024_json = requests.get(
    "https://tabular-api.data.gouv.fr/api/resources/1050324f-a0ce-4a06-94c5-e2f6805fdb74/data/"
).json()

df = pd.DataFrame(election_2024_json["data"])

con = duckdb.connect("assets/data.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS election_2024_json AS SELECT * FROM df")

print(con.execute("SELECT * FROM election_2024_json LIMIT 5").fetchdf())
