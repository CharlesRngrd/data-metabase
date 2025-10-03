import json
import requests

# URL retrieved from :
# https://www.data.gouv.fr/datasets/contours-geographiques-des-circonscriptions-legislatives
map_json = requests.get(
    "https://www.data.gouv.fr/api/1/datasets/r/8b681b69-739c-47eb-a96b-06e8e2d8dc08"
).json()


# DOM-TOM codeDepartement always starts with Z :
map_json["features"][:] = [
    f
    for f in map_json["features"]
    if not f["properties"]["codeDepartement"].startswith("Z")
]

for feature in map_json["features"]:
    property = feature["properties"]

    property["labelCirconscription"] = (
        f"{property['nomDepartement']} - {property['nomCirconscription']}"
    )

with open("assets/map.geojson", "w") as json_file:
    json.dump(map_json, json_file)
