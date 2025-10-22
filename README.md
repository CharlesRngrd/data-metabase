# Installation

## Metabase + Pandas

### Installation des dépendances python

- Executer `pip install -r requirements.txt`

### Chargement des données

- Executer `python src/data_ingestion.py`
- Executer `python src/data_transformation/with_pandas.py`

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

## Metabase + pySpark

### Option 1 avec Conda

- Executer `conda create -n pyspark-env python=3.10 openjdk=17 pyspark=4.0.1 jupyterlab duckdb`
- Executer `conda activate pyspark-env`

### Option 1 sans Conda (possibles erreurs avec python 3.13)

**Installation des dépendances python**

- Executer `pip install -r requirements.txt`

**Installation de Java**

- Téléchargement de Java 21 https://www.oracle.com/fr/java/technologies/downloads/#java21
- Téléchargement de Hadoop Winutils https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe
- Ajout Java 21 dans les variables d'environnement :
    - `setx JAVA_HOME "C:\Program Files\Java\jdk-21"`
    - `setx PATH "%JAVA_HOME%\bin;%PATH%"`
