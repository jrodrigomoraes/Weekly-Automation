# Usar a imagem oficial do Great Expectations
FROM greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0

# Instalar o Apache Airflow e outras dependências necessárias
RUN pip install apache-airflow==2.9.1 \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    pandas \
    matplotlib \
    seaborn \
    python-dotenv \
    requests \
    reportlab

# Configurações do Airflow (caso necessário)
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=$PYTHONPATH:/opt/airflow

# Definir o diretório de trabalho
WORKDIR /opt/airflow

# Iniciar o shell interativo ou a execução do Airflow por padrão
CMD ["bash"]