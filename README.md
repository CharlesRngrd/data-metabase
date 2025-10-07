# Installation

## Metabase

### Installation des dépendances python

- Executer `pip install -r requirements.txt`

### Chargement des données

- Executer `python src/data_ingestion.py`
- Executer `python src/data_transformation.py`

### Chargement de Metabase

- Executer `docker-compose up -d`

### Chargement de la carte

- Executer `python src/map.py`
- Publier la carte sur GitHub
- Charger la carte dans Metabase à partir de l'URL de la carte sur GitHub

**Attention**

- GitHub exige une 200 et non une 302.<br>
Voici l'URL à utiliser : https://raw.githubusercontent.com/CharlesRngrd/data_demo/refs/heads/main/assets/map.geojson

- La carte dépasse les 5 Mo autorités par défaut sur Metabase<br>
Voici la variable d'environnement à modifier : `MB_CUSTOM_MAPS_MAX_FILE_SIZE_MB`
