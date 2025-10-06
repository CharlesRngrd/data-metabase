# Installation

## Metabase

### Chargement des données

- Executer `python src/data.py`

### Chargement de Metabase

- Executer `docker build -t metabase-duckdb .`
- Executer `docker run -d -p 3000:3000 -e "MB_CUSTOM_MAPS_MAX_FILE_SIZE_MB=20" -v ${PWD}/assets/data.duckdb:/app/data.duckdb --name metabase-duckdb metabase-duckdb`

### Chargement de la carte

- Executer `python src/map.py`
- Publier la carte sur GitHub
- Charger la carte dans Metabase à partir de l'URL de la carte sur GitHub

**Attention**

- GitHub exige une 200 et non une 302.<br>
Voici l'URL à utiliser : https://raw.githubusercontent.com/CharlesRngrd/data_demo/refs/heads/main/assets/map.geojson

- La carte dépasse les 5 Mo autorités par défaut sur Metabase<br>
Voici la variable d'environnement à modifier : `MB_CUSTOM_MAPS_MAX_FILE_SIZE_MB`

- Le plugin duckdb est compatible avec la version `duckdb=1.3.1`
