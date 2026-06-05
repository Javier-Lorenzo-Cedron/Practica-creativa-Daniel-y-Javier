# Practica-creativa-Daniel-y-Javier

Guía para desplegar el escenario completo partiendo solo de la carpeta del
proyecto, SIN las carpetas `models/`, `data/` ni `flight_prediction/target/`
(se regeneran o descargan durante el despliegue).

Todo el escenario corre en Docker mediante un único `docker-compose.yml`:
Cassandra, MinIO, Kafka, el servidor web (Flask) y un cluster Spark
(1 master + 2 workers). El job de predicción se ejecuta también en el cluster,
como un servicio bajo demanda (perfil "job").

IMPORTANTE: sigue los pasos EN ORDEN. Cada paso prepara lo que el siguiente
necesita (hay dependencias entre datos, modelos y el job).

==========================================================================
## 0. Requisitos previos en la máquina destino
==========================================================================
- Docker y Docker Compose
- Java 17 y sbt (para compilar el job de Scala)
- Python 3.10 con virtualenv (para los scripts de preparación)
- curl (para descargar los datos)

==========================================================================
## 1. Entorno de Python
==========================================================================

Desde la raíz del proyecto (practica_creativa/):

    python3 -m venv env
    source env/bin/activate
    pip install -r requirements.txt

(pyspark aporta el spark-submit que usan los scripts de preparación de los
pasos 7 y 8.)

==========================================================================
## 2. Descargar los datos (genera la carpeta data/)
==========================================================================
    
    bash resources/download_data.sh

Debe dejar en data/:
- simple_flight_delay_features.jsonl.bz2
- origin_dest_distances.jsonl

==========================================================================
## 3. Compilar el job de Scala (genera flight_prediction/target/)
==========================================================================
    
    cd flight_prediction
    sbt package
    cd ..

Debe terminar con [success]. Genera el JAR:
flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar

(El JAR es necesario antes de arrancar el servicio spark-job, porque se
monta como volumen en el contenedor.)

==========================================================================
## 4. Levantar la infraestructura (todo menos el job)
==========================================================================
    
    docker compose up -d

Levanta: cassandra, minio, kafka, web, spark-master, spark-worker-1,
spark-worker-2. El servicio spark-job NO arranca aquí (tiene perfil "job").

Verifica que están TODOS arriba:

    docker compose ps

Si faltara alguno (p. ej. web a veces no arranca al primer intento al
encender la VM), levántalo explícitamente:

    docker compose up -d web

Espera a que cassandra aparezca como "Up (healthy)" antes de seguir.

Verifica el cluster Spark en el navegador:
    http://localhost:8080
Debe aparecer el master ALIVE con 2 workers registrados.

==========================================================================
## 5. Crear los topics de Kafka
==========================================================================
    
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic flight-delay-ml-request
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic flight-delay-ml-response

Verifica:
    
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

==========================================================================
## 6. Preparar Cassandra (keyspace, tablas e importar distancias)
==========================================================================

Crear keyspace y tablas:

    docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE agile_data_science; CREATE TABLE IF NOT EXISTS origin_dest_distances (origin text, dest text, distance int, PRIMARY KEY (origin, dest)); CREATE TABLE IF NOT EXISTS flight_delay_predictions (uuid text PRIMARY KEY, origin text, dest text, prediction int, timestamp timestamp);"

Importar las distancias (con el entorno activado; tarda ~2 min):

    source env/bin/activate
    python3 import_to_cassandra.py

Verifica (debe dar ~4696):
    
    docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances"

==========================================================================
## 7. Crear la tabla Iceberg en el Lakehouse (MinIO)
==========================================================================

El bucket 'lakehouse' lo crea MinIO al arrancar. Si no existiera, créalo en
la consola web (http://localhost:9001, admin/admin123) o con:
    
    docker exec minio mc alias set local http://localhost:9000 admin admin123
    
    docker exec minio mc mb --ignore-existing local/lakehouse

Crear la tabla Iceberg con los datos de vuelos:

    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1 spark_iceberg.py

Debe terminar con "Tabla Iceberg creada en MinIO correctamente".
Verifica en http://localhost:9001 -> lakehouse -> warehouse/

==========================================================================
## 8. Entrenar los modelos (genera models/ en local y en el Lakehouse)
==========================================================================

Lee los datos del Lakehouse y guarda los modelos en local (carpeta models/)
y en MinIO. Tarda varios minutos (entrena un RandomForest sobre 457k filas):

    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1 resources/train_spark_mllib_model.py .

Al terminar, comprueba que existe la carpeta models/ con los .bin:
    ls models/

(IMPRESCINDIBLE antes del paso 9: el cluster Spark monta models/ como volumen
y el job carga los modelos desde ahí.)

==========================================================================
## 9. Lanzar el job de predicción en el cluster (servicio spark-job)
==========================================================================

Con los datos, topics y modelos ya listos, arranca el job bajo demanda:

    docker compose --profile job up -d spark-job

Sigue los logs hasta que cargue los modelos y se quede escuchando:

    docker compose logs -f spark-job

En http://localhost:8080 aparecerá la aplicación corriendo, con tareas
repartidas entre los 2 workers (modo distribuido).

==========================================================================
## 10. Probar el escenario completo
==========================================================================

Abrir en el navegador:
    http://localhost:5001/flights/delays/predict_kafka

Rellenar el formulario (valores por defecto ATL -> SFO) y pulsar Submit.
La predicción debe aparecer en la página sin recargar (vía WebSocket).

Verificar que se guardó en Cassandra:
    docker exec cassandra cqlsh -e "SELECT * FROM agile_data_science.flight_delay_predictions LIMIT 50"

==========================================================================
## Orden de dependencias (resumen)
==========================================================================

env Python -> descargar data -> compilar JAR -> docker compose up (infra) ->
topics Kafka -> Cassandra (keyspace+tablas+distancias) ->
Iceberg (datos en Lakehouse) -> entrenar modelos -> lanzar spark-job -> probar

==========================================================================
## Notas
==========================================================================

- Cassandra y Kafka NO tienen volumen: si se apagan o se recrean los
  contenedores, hay que repetir los pasos 5 y 6 (topics y datos de Cassandra)
  al volver a arrancar. MinIO sí tiene volumen (minio_data), conserva los
  datos Iceberg y los modelos entre reinicios.
- El servicio spark-job monta como volúmenes el JAR
  (flight_prediction/target/scala-2.13) y la carpeta models/. Por eso los
  pasos 3 y 8 deben completarse ANTES del paso 9.
- Los workers del cluster también montan models/, porque cada executor lee
  los modelos en el worker donde se ejecuta la tarea.
- El job está parametrizado por variables de entorno (SPARK_MASTER,
  KAFKA_BROKER, CASSANDRA_HOST, BASE_PATH), definidas en el servicio
  spark-job del docker-compose para apuntar a los nombres de servicio.
