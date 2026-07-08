# Practica-creativa-Daniel-y-Javier

Guía para desplegar el escenario completo partiendo solo de la carpeta del proyecto, **SIN las carpetas `models/`, `data/` ni `flight_prediction/target/`** (se regeneran o descargan durante el proceso). 
Todo el escenario corre en Docker mediante un único `docker-compose.yml`: **Cassandra, MinIO, Kafka, el servidor web (Flask) y un cluster Spark** (1 master + 2 workers). El **job de predicción** se ejecuta también en el cluster, como un servicio bajo demanda (perfil `"job"`).
Además, parte de la preparación se hace ya de forma automática al levantar la infraestructura:
- `kafka-init` crea los topics de Kafka
- `cassandra-init` crea el keyspace, las tablas e importa las distancias
- `minio-init` crea el bucket `lakehouse`
- `iceberg-init` crea la tabla Iceberg en MinIO
## Importante
Sigue los pasos **en orden**. Cada paso prepara lo que el siguiente necesita.

## Instalación automática (recomendado)

El script `setup.sh` automatiza todos los pasos previos a levantar la infraestructura Docker. Detecta tu sistema operativo e instala/verifica todas las dependencias necesarias.

### Sistemas soportados

- Ubuntu / Debian
- Fedora
- CentOS / RHEL
- Arch Linux
- macOS (requiere Homebrew)

### Uso


**Descargar el script (si no tienes el repositorio clonado)**

    curl -O https://raw.githubusercontent.com/Javier-Lorenzo-Cedron/Practica-creativa-Daniel-y-Javier/main/setup.sh


**Dar permisos y ejecutar**

    chmod +x setup.sh

    ./setup.sh

### ¿Qué hace el script?

- Paso 0: Instala Docker, Java 17, sbt, Python 3.10+, curl y git si faltan
- Paso 1: Clona el repositorio (si no existe)
- Paso 2: Crea el entorno virtual Python e instala dependencias (`requirements.txt`)
- Paso 3: Descarga los datos (`resources/download_data.sh`)
- Paso 4: Compila el JAR de Scala (`sbt package`)


El script es idempotente: puede ejecutarse múltiples veces sin problemas. Si algo ya está instalado o completado, lo detecta y continúa.

Una vez finalizado, continúa con el paso 4 de esta guía.

## Instalación manual
Si prefieres instalar todo manualmente, sigue los pasos a continuación.

==========================================================================
### 0. Requisitos previos en la máquina destino
==========================================================================
- Docker y Docker Compose
- Java 17 y sbt (para compilar el job de Scala)
- Python 3.10 con virtualenv (para los scripts de preparación)
- curl (para descargar los datos)

==========================================================================
### 1. Entorno de Python
==========================================================================

Desde la raíz del proyecto (practica_creativa/):

    python3 -m venv env
    source env/bin/activate
    pip install -r requirements.txt

(pyspark aporta el spark-submit que usan los scripts de preparación de los
pasos 7 y 8.)

==========================================================================
### 2. Descargar los datos (genera la carpeta data/)
==========================================================================
    
    bash resources/download_data.sh

Debe dejar en data/:
- simple_flight_delay_features.jsonl.bz2
- origin_dest_distances.jsonl

==========================================================================
### 3. Compilar el job de Scala (genera flight_prediction/target/)
==========================================================================
    
    cd flight_prediction
    sbt package
    cd ..

Debe terminar con [success]. Genera el JAR:
flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar

(El JAR es necesario antes de arrancar el servicio spark-job, porque se
monta como volumen en el contenedor.)

==========================================================================
## 4. Levantar la infraestructura base (todo menos el train y el job)
==========================================================================
    
    docker compose up -d

Levanta: cassandra, cassandra-init, minio, minio-init, kafka, kafka-init, web, spark-master, spark-worker-1, spark-worker-2, iceberg-init, **mlflow** y **airflow**.. No arranca todavía spark-job, porque ese servicio va bajo perfil "job" y se lanza después, cuando ya existan los modelos.

Verifica que están TODOS arriba:

    docker compose ps

Debes esperar a que:
- cassandra aparezca como healthy
- cassandra-init termine en estado Exited (0)
- kafka-init termine en estado Exited (0)
- minio-init termine en estado Exited (0)
- iceberg-init termine en estado Exited (0)

Si faltara alguno, levántalo explícitamente:

    docker compose up -d <contenedor>
    
Si web se queda reiniciendose al comprobar los contenedores, esto es normal, continua con el siguiente paso.

Verifica el cluster Spark en el navegador:
    http://IP:8080
Debe aparecer el master ALIVE con 2 workers registrados.

==========================================================================
## 5. Verificar los topics de Kafka
==========================================================================

Puedes comprobar que los topics existen:
    
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

Deben aparecer:
- flight-delay-ml-request
- flight-delay-ml-response

==========================================================================
## 6. Crear flujo NiFi (si no está ya creado)
==========================================================================

Antes de nada comprobar que en el contenedor *nifi* en el archivo docker-compose.yml su variable de entorno NIFI_WEB_PROXY_HOST hace referencia al IP (o localhost) de la máquina que se esté utilizando (a veces estas IPs cambian al detener e iniciar una VM en GCloud), y si no es así actualizar esa variable:

**En local:**

    git add .
    git commit -m "Flink"
    git push -u origin *RamaUtilizada*

**En VM de GCloud:**

    git checkout *RamaUtilizada*
    git pull

Ve a *https://IP:8443/nifi*:
- usuario: admin
- contraseña: adminadminadmin

Si el flujo no lo tienes ya creado descárgate el archivo /nifi/config_definitiva.xml

Después en la página de NiFi, en el operador, le das al botón de upload template y eliges el archivo que acabas de descargar. 

Una vez terminada la descarga buscas en la barra de arriba la opción de *Template* y la arrastras a la cuadrilla, te mostrará los diferentes templates disponibles y tu de ahí eliges el que has descargado. 

Una vez con el flujo configurado sólo le tienes que ir dando Start a los diferentes procesadores yendo de derecha a izquierda.

==========================================================================
## 7. Verificar Cassandra
==========================================================================

Puedes comprobar que las distancias se han cargado (debe dar ~4696):

    docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;"

==========================================================================
## 8. Verificar MinIO
==========================================================================

Abre http://IP:9001

Credenciales por defecto:
- usuario: admin
- contraseña: admin123

Debe existir el bucket *lakehouse*, y dentro deberían haberse creado los datos de Iceberg.

==========================================================================
## 9. Verificar MLflow
==========================================================================

MLflow registra los entrenamientos del modelo en un servidor independiente, de forma que cada ejecución del train queda almacenada con:
- parámetros
- métricas
- artefactos/modelos

Abrir en el navegador:

    http://IP:5000

Debe aparecer la interfaz web de MLflow.

Notas:
- En esta práctica MLflow usa SQLite como backend y un volumen Docker (`mlflow_data`) para almacenar la base de datos y los artefactos.
- Si la interfaz web no carga desde fuera de la máquina, comprobar que el puerto 5000 está permitido en el firewall.


==========================================================================
## 10. Entrenar los modelos (models/ en el Lakehouse)
==========================================================================

Ejecuta:

    docker compose --profile train up --build spark-train -d

Si quieres seguir los logs:
    
    docker compose logs -f spark-train

Este proceso tarda por lo general varios minutos. Puedes seguirlo también en http://IP:8080 viendo los workers spark (suele tardar unos 10-20 segundos en aparecer como running application).

Además, en MLflow (http://IP:5000) debe aparecer un nuevo experimento o un nuevo run con:
- parámetros del entrenamiento
- métrica `accuracy`
- y el modelo registrado como artefacto

Cuando termine puede comprobar en MiniO (http://IP:9001) que la carpeta models está creada con todos los modelos dentro .bin

(IMPRESCINDIBLE antes del paso 9: el cluster Spark monta models/ como volumen
y el job carga los modelos desde ahí.)

==========================================================================
## 11. Reentrenamiento orquestado con Airflow
==========================================================================

El proyecto incorpora **Apache Airflow** para orquestar el reentrenamiento del modelo en el cluster Spark.

Airflow ejecuta un `spark-submit` contra el cluster y, como el entrenamiento ya integra MLflow, cada ejecución queda registrada automáticamente en MLflow. En MLflow http://IP:5000 debe aparecer un nuevo experimento o un nuevo run con:
- parámetros del entrenamiento
- métrica `accuracy`
- y el modelo registrado como artefacto

Abrir en el navegador:

    http://IP:8090

Credenciales por defecto:
- usuario: `admin`
- contraseña: `admin`

Debe aparecer el DAG:

- `flight_delay_retraining`

### Ejecutar el DAG

1. Activar el DAG con el interruptor de la interfaz.
2. Pulsar el botón **Play / Trigger DAG**.
3. Entrar en la tarea `retrain_model` para ver sus logs (si falla, para averiguar por qué ha fallado).

### Verificación

La ejecución se considera correcta cuando:
- la tarea `retrain_model` aparece en **verde (success)** en Airflow
- y en MLflow (http://IP:5000) aparece un **nuevo run** del experimento

Notas:
- Airflow usa SQLite y `SequentialExecutor`, lo cual es suficiente para esta práctica.
- Si Airflow no carga desde fuera de la máquina, comprobar que el puerto **8090** está permitido en el firewall.
- Si el DAG falla, revisar primero los logs de la tarea desde la interfaz de Airflow.


==========================================================================
## 12. Lanzar el job de predicción en el cluster (servicio spark-job)
==========================================================================

Con los datos, topics y modelos ya listos, arranca el job de predicción basado en Spark bajo demanda:

    docker compose --profile job up -d spark-job

Sigue los logs hasta que cargue los modelos y se quede escuchando:

    docker compose logs -f spark-job

En http://IP:8080 aparecerá la aplicación corriendo, con tareas
repartidas entre los 2 workers (modo distribuido).
Este es el modo de predicción por defecto del proyecto.

==========================================================================
## 13. Predicción alternativa con Apache Flink (sustituye a Spark)
==========================================================================

Como alternativa al job de predicción con Spark, el proyecto permite ejecutar la inferencia con **Apache Flink**.

En este modo:
- el entrenamiento sigue realizándose con Spark
- el modelo entrenado se exporta a un JSON intermedio (`data/flink_model.json`)
- Flink consume peticiones desde Kafka
- Flink escribe la predicción en Cassandra y en el topic de respuesta de Kafka

La cadena completa queda así:

Web -> Kafka -> Flink -> Cassandra + Kafka response -> WebSocket -> Web

### 13.1 Levantar el cluster Flink

Arrancar:

    docker compose up -d flink-jobmanager flink-taskmanager-1 flink-taskmanager-2

La interfaz de Flink queda disponible en:

    http://IP:8092

### 13.2 Exportar el modelo entrenado a JSON

Antes de lanzar el job de Flink, hay que exportar el modelo de Spark MLlib al fichero:

    data/flink_model.json

Ejecutar:

    docker run --rm \
      --network practica-creativa-daniel-y-javier_default \
      -v "$PWD:/opt/project" \
      practica-creativa-daniel-y-javier-spark-train:latest \
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=admin \
      --conf spark.hadoop.fs.s3a.secret.key=admin123 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      --conf spark.hadoop.fs.s3a.change.detection.mode=none \
      --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1 \
      /opt/project/resources/export_model_for_flink.py /opt/project

Verificar que el fichero se ha generado:

    ls -lh data/flink_model.json
    head -40 data/flink_model.json

### 13.3 Compilar el job de Flink

El proyecto de Flink está en:

    flink_prediction/

Compilar el fat JAR con:

    docker run --rm \
      -v "$PWD/flink_prediction:/proyecto" \
      -w /proyecto \
      hseeberger/scala-sbt:17.0.2_1.6.2_2.12.15 \
      sbt clean assembly

Debe generarse el fichero:

    flink_prediction/target/scala-2.12/flink-flight-predictor-assembly-0.1.0.jar

### 13.4 Parar el predictor Spark (si lo has ejecutado antes)

Como Spark y Flink consumirían el mismo topic de peticiones, antes de lanzar Flink hay que parar el predictor Spark:

    docker stop spark-job

### 13.5 Lanzar el job de Flink

Ejecutar:

    docker exec flink-jobmanager flink run \
      -c es.upm.dit.ging.predictor.FlinkPredictor \
      /opt/flink/job/target/scala-2.12/flink-flight-predictor-assembly-0.1.0.jar

Comprobar si está corriendo:

    docker exec flink-jobmanager flink list

### 13.6 Verificación de punta a punta

Abrir en el navegador:

    http://IP:5001/flights/delays/predict_kafka

Rellenar el formulario y enviar una predicción.

Comprobar que la predicción se ha guardado en Cassandra:

    docker exec cassandra cqlsh -e \
    "SELECT uuid, origin, dest, prediction, timestamp FROM agile_data_science.flight_delay_predictions LIMIT 20;"

Y opcionalmente comprobar el topic de respuesta de Kafka:

    docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 \
      --topic flight-delay-ml-response \
      --from-beginning \
      --max-messages 10

Si todo funciona, queda demostrada la cadena:

Web -> Kafka -> Flink -> Cassandra + Kafka response -> WebSocket -> Web

Notas:
- Los `TaskManager` montan `./data:/opt/flink/data`, porque el job necesita leer `flink_model.json`.
- El cluster Flink usa Scala 2.12, independiente del proyecto de predicción con Spark/Scala 2.13.
- Si el job de Flink no aparece como `RUNNING`, revisar logs y verificar que `data/flink_model.json` existe.


==========================================================================
## 14. Probar el escenario completo
==========================================================================

Hay dos modos de predicción posibles:
### Opción A: predicción con Spark
Usar el servicio `spark-job` (paso 12).
### Opción B: predicción con Flink
Usar el cluster Flink y el job `FlinkPredictor` (paso 13).

En ambos casos, abrir en el navegador:
    http://IP:5001/flights/delays/predict_kafka

Rellenar el formulario (valores por defecto ATL -> SFO) y pulsar Submit.
La predicción debe aparecer en la página sin recargar (vía WebSocket).

Verificar que se guardó en Cassandra:
    
    docker exec cassandra cqlsh -e "SELECT * FROM agile_data_science.flight_delay_predictions LIMIT 50"

==========================================================================
## Orden de dependencias (resumen)
==========================================================================

env Python -> descargar data -> compilar JAR Spark -> docker compose up -d ->
esperar inicialización automática -> verificar MinIO / Cassandra / Kafka / MLflow ->
entrenar modelos con Spark (manual o Airflow) ->
elegir modo de predicción:
  - Spark: lanzar spark-job
  - Flink: exportar modelo -> compilar jar Flink -> lanzar job Flink
-> probar la aplicación


==========================================================================
## Notas
==========================================================================

- Cassandra y Kafka no tienen volumen persistente. Si se eliminan o recrean sus contenedores, habrá     que volver a levantar la infraestructura para que sus servicios *-init rehagan la inicialización.
- MinIO sí tiene volumen persistente (minio_data), así que conserva el Lakehouse y los modelos         almacenados en S3/MinIO entre reinicios.
- El servicio spark-job monta como volúmenes el JAR
  (flight_prediction/target/scala-2.13) y la carpeta models/. Por eso los
  pasos 3 y 8 deben completarse ANTES del paso 9.
- Los workers del cluster también montan models/, porque los executors pueden necesitar acceder a esos modelos durante la ejecución distribuida.
- El job está parametrizado por variables de entorno (SPARK_MASTER,
  KAFKA_BROKER, CASSANDRA_HOST, BASE_PATH), definidas en el servicio
  spark-job del docker-compose para apuntar a los nombres de servicio.
- MLflow queda disponible en el puerto `5000` y almacena los runs del entrenamiento en el volumen `mlflow_data`.
- Airflow queda disponible en el puerto `8090` y usa SQLite + `SequentialExecutor`
- Flink queda disponible en el puerto `8092` y se usa únicamente para la predicción alternativa.
- Si se utiliza Flink, debe detenerse previamente `spark-job` (si se ha arrancado), ya que ambos consumirían del mismo topic `flight-delay-ml-request`.
- El fichero `data/flink_model.json` se genera a partir del modelo entrenado con Spark y es imprescindible antes de lanzar el job de Flink.

