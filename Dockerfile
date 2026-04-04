FROM apache/airflow:3.1.7-python3.11

USER root

# Instala o OpenJDK 17 necessario para o PySpark que rodará localmente no container do Airflow
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Define a variável de ambiente JAVA_HOME para o OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
