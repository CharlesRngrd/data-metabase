FROM openjdk:21-bullseye

ENV MB_PLUGINS_DIR=/home/plugins/

ADD https://downloads.metabase.com/v0.56.9/metabase.jar /home
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/1.4.1.0/duckdb.metabase-driver.jar /home/plugins/

RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

CMD ["java", "-jar", "/home/metabase.jar"]
