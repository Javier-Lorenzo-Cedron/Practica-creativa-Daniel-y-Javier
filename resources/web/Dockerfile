FROM python:3.10-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias primero (aprovecha la cache de Docker si no cambian)
COPY requirements-web.txt .
RUN pip install --no-cache-dir -r requirements-web.txt

# Copiar el codigo del servidor web (predict_flask.py, predict_utils.py,
# config.py, templates/, static/)
COPY . .

# PROJECT_HOME lo necesita predict_flask.py para os.environ["PROJECT_HOME"]
ENV PROJECT_HOME=/app

# Puerto del servidor Flask
EXPOSE 5001

CMD ["python3", "predict_flask.py"]
