# Despliegue en Google Cloud VM

Esta guía explica cómo desplegar la práctica en una máquina virtual de **Google Cloud Platform (GCP)** accediendo por **SSH** a la instancia.

**Importante:** este documento complementa al `README` principal del repositorio.  
El `README` original sigue siendo válido para el despliegue general/local; esta guía añade los pasos y consideraciones específicas para una VM de Google Cloud.

---

## 1. Requisitos previos

Antes de empezar, la VM debe cumplir lo siguiente:

- Sistema operativo basado en **Ubuntu/Debian**
- Acceso por **SSH**
- Conexión a Internet para descargar dependencias y datos
- Permisos para abrir reglas de firewall en Google Cloud

Además, se necesitan estas herramientas en la VM:

- **Docker**
- **Docker Compose**
- **Java 17**
- **sbt**
- **Python 3.10** con `venv`
- **curl**
- **git**

---

## 2. Clonar el repositorio

Conéctate por SSH a la VM y clona el proyecto:

```bash
cd ~
git clone https://github.com/Javier-Lorenzo-Cedron/Practica-creativa-Daniel-y-Javier.git
cd Practica-creativa-Daniel-y-Javier``` 


---

## 3. Instalar dependencias del sistema

Actualizar paquetes:

  sudo apt update && sudo apt upgrade -y

Instalar herramientas base:

  sudo apt install -y git curl openjdk-17-jdk python3.10 python3.10-venv python3-pip docker.io ca-certificates gnupg lsb-release

Habilitar y arrancar Docker:

  sudo systemctl enable docker
  sudo systemctl start docker
  sudo usermod -aG docker $USER

Importante: tras añadir el usuario al grupo docker, conviene cerrar la sesión SSH y volver a entrar.
Comprobar instalación:

  docker --version
  docker compose version
  java -version
  python3.10 --version

---

## 4. Instalar sbt

Añadir repositorios:

  echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
  echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/scalasbt_old.list
  curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/sbt.gpg > /dev/null
  sudo apt update
  sudo apt install -y sbt

Comprobar:

 sbt --version

---

##  5. Crear el entorno Python

Desde la raíz del proyecto:

  python3.10 -m venv env
  source env/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt

---

## 6. Descargar los datos

  bash resources/download_data.sh

Se debe generar la carpeta data/ con estos archivos:
- simple_flight_delay_features.jsonl.bz2
- origin_dest_distances.jsonl

Comprobación:

  ls data

---

## 7. Compilar el job de Scala

Entrar en el subproyecto y generar el JAR:

  cd flight_prediction
  sbt package
  cd ..

Comprobar que existe el JAR:

  ls flight_prediction/target/scala-2.13/

---

## 8. Levantar la infraestructura base

Arrancar los servicios principales:

  docker compose up -d

Comprobar estado:

  docker compose ps

Deben quedar correctamente levantados:
- cassandra
- kafka
- minio
- spark-master
- spark-worker-1
- spark-worker-2
- web

Y deben completar correctamente los servicios de inicialización:
- cassandra-init
- kafka-init
- minio-init
- iceberg-init

---

## 9. Verificaciones básicas

**Kafka**:

  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

Deben aparecer los topics:
- flight-delay-ml-request
- flight-delay-ml-response

**Cassandra**:

  docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;"

Debe devolver aproximadamente 4696.

**Spark**:
En local dentro de la VM, la interfaz debe responder en:

  curl http://localhost:8080

**MiniO**:
En local dentro de la VM:

  curl http://localhost:9001

---

## 10. Entrenar los modelos

  docker compose --profile train up --build spark-train -d

  docker compose logs -f spark-train

Cuando termine, los modelos deben haberse creado en MinIO.

---

## 11. Lanzar el job de predicción

  docker compose --profile job up -d spark-job

  docker compose logs -f spark-job

---

## 12. Abrir puertos en Google Cloud

**Regla de firewall**
Crear una regla de entrada con:
- *Direction*: Ingress
- *Action*: Allow
- *Targets*: la instancia o una etiqueta de red
- *Source IPv4 ranges*: 0.0.0.0/0 para pruebas, o tu IP pública si quieres restringir acceso
- *Protocols/ports*: tcp:5001,8080,9001

Si la regla usa una network tag, esa misma etiqueta debe estar asignada a la VM.
Por ejemplo, si la regla se aplica a la etiqueta *practica-creativa*, la instancia debe tener exactamente esa etiqueta en su configuración de red.

---

## 13. Obtener la IP externa de la VM

Desde la consola de la VM:

  curl ifconfig.me

---

## 14. Acceso desde navegador

Sustituye IP_EXTERNA por la IP pública real de tu VM.

**Aplicación web**

  http://IP_EXTERNA:5001/flights/delays/predict_kafka

**Spark UI**

  http://IP_EXTERNA:8080

**MinIO**

  http://IP_EXTERNA:9001

Credenciales por defecto de MinIO:
- usuario: admin
- contraseña: admin123

---

## 15. Verificar que la aplicación responde

Dentro de la VM, esta ruta debe responder correctamente:

  curl -i http://localhost:5001/flights/delays/predict_kafka

Debe devolver:

  HTTP/1.1 200 OK

---

## 16. Probar la predicción completa

  http://IP_EXTERNA:5001/flights/delays/predict_kafka

Pulsar submit y esperar a que aparezca la predicción sin recargar la página.

---

## 17. Verificar persistencia en Cassandra

Tras realizar una predicción:

  docker exec cassandra cqlsh -e "SELECT * FROM agile_data_science.flight_delay_predictions LIMIT 50;"

---

## 18. Resumen del orden de despliegue

Clonar repositorio
-> instalar dependencias
-> crear entorno Python
-> descargar datos
-> compilar JAR
-> docker compose up -d
-> comprobar inicialización
-> entrenar modelos
-> arrancar spark-job
-> abrir puertos en GCP
-> acceder desde navegador
