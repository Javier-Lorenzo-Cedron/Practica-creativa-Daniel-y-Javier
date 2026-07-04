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

Levanta: cassandra, cassandra-init, minio, minio-init, kafka, kafka-init, web, spark-master, spark-worker-1, spark-worker-2, iceberg-init. No arranca todavía spark-job, porque ese servicio va bajo perfil "job" y se lanza después, cuando ya existan los modelos.

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
    http://localhost:8080
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

Ve a *https://localhost:8443/nifi*:
- usuario: admin
- contraseña: adminadminadmin

Si el flujo no lo tienes ya creado descárgate el archivo /nifi/config_definitiva.xml
Después en la página de NiFi, en el operador, le das al botón de upload template y eliges el archivo que acabas de descargar. Una vez terminada la descarga buscas en la barra de arriba la opción de *Template* y la arrastras a la cuadrilla, te mostrará los diferentes templates disponibles y tu de ahí eliges el que has descargado. Una vez con el flujo configurado sólo le tienes que ir dando Start a los diferentes procesadores yendo de derecha a izquierda.

==========================================================================
## 7. Verificar Cassandra
==========================================================================

Puedes comprobar que las distancias se han cargado (debe dar ~4696):

    docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;"

==========================================================================
## 8. Verificar MinIO
==========================================================================

Abre http://localhost:9001

Credenciales por defecto:
- usuario: admin
- contraseña: admin123

Debe existir el bucket *lakehouse*, y dentro deberían haberse creado los datos de Iceberg.

==========================================================================
## 9. Entrenar los modelos (models/ en el Lakehouse)
==========================================================================

Ejecuta:

    docker compose --profile train up --build spark-train -d

Si quieres seguir los logs:
    
    docker compose logs -f spark-train

Este proceso tarda por lo general varios minutos. Puedes seguirlo también en http://localhost:8080 viendo los workers spark (suele tardar unos 10-20 segundos en aparecer como running application)

Cuando termine puede comprobar en MiniO (http://localhost:9001) que la carpeta models está creada con todos los modelos dentro .bin

(IMPRESCINDIBLE antes del paso 9: el cluster Spark monta models/ como volumen
y el job carga los modelos desde ahí.)

==========================================================================
## 10. Lanzar el job de predicción en el cluster (servicio spark-job)
==========================================================================

Con los datos, topics y modelos ya listos, arranca el job bajo demanda:

    docker compose --profile job up -d spark-job

Sigue los logs hasta que cargue los modelos y se quede escuchando:

    docker compose logs -f spark-job

En http://localhost:8080 aparecerá la aplicación corriendo, con tareas
repartidas entre los 2 workers (modo distribuido).

==========================================================================
## 11. Probar el escenario completo
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

env Python -> descargar data -> compilar JAR -> docker compose up -d ->
esperar inicialización automática -> entrenar modelos -> lanzar spark-job -> probar la aplicación

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
