#Usa imagem oficial do Airflow
FROM apache/airflow:2.9.2-python3.11

#Muda para root para instalar pacotes de sistema
USER root

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    libgirepository1.0-dev \
    libcairo2-dev \
    pkg-config \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

#Volta para o usuário airflow
USER airflow

#Copia requirements.txt e instala dependências Python
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY .env /opt/airflow/.env