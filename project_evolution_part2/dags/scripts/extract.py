#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import psycopg2
import logging
from dotenv import load_dotenv

#Configuração do logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Carregando variáveis de ambiente
load_dotenv('/opt/airflow/.env')  # caminho absoluto no container Airflow
senha_db = os.getenv("DB_PASSWORD")

if not senha_db:
    raise ValueError("Senha não encontrada, verifique o arquivo .env")

csv_path = '/opt/airflow/csv/novas_vendas.csv'  # caminho dentro do container

def extract_data():
    logger.info("Iniciando a extração de dados")

    if not os.path.exists(csv_path):
        logger.warning(f"Arquivo CSV não encontrado em {csv_path}. Extração interrompida!")
        return pd.DataFrame()

    df = pd.read_csv(csv_path, encoding='latin1')
    logger.info(f"Arquivo CSV carregado com {len(df)} linhas.")
    logger.info(f"Exemplo dos dados CSV:\n{df.head()}")

    try:
        conn = psycopg2.connect(
            host='postgres',
            database='vendas_db',
            user='airflow',
            password=senha_db
        )
        cur = conn.cursor()
        logger.info("Conexão com o banco de dados estabelecida.")
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco: {e}", exc_info=True)
        return pd.DataFrame()

    # Processa clientes
    clientes_raw = df[['nome', 'email', 'cidade', 'estado']].drop_duplicates()

    # Limpeza dos dados (evita erro de CHAR(2) no banco)
    clientes_raw['estado'] = clientes_raw['estado'].astype(str).str.strip().str[:2]

    clientes_raw['flag_valid'] = clientes_raw['email'].apply(
        lambda x: False if pd.isna(x) or str(x).strip().lower() in ['nan', ''] else True
    )
    logger.info(f"{len(clientes_raw)} clientes únicos extraídos do CSV.")

    clientes_data = []
    for idx, row in clientes_raw.iterrows():
        clientes_data.append((
            row['nome'],
            row['email'],
            row['cidade'],
            row['estado'],
            row['flag_valid']
        ))
        logger.debug(f"Cliente {idx}: {clientes_data[-1]}")

    try:
        cur.executemany("""
            INSERT INTO clientes_raw (nome, email, cidade, estado, flag_valid)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET
                nome = EXCLUDED.nome,
                cidade = EXCLUDED.cidade,
                estado = EXCLUDED.estado,
                flag_valid = EXCLUDED.flag_valid
        """, clientes_data)
        conn.commit()
        logger.info(f"{len(clientes_data)} clientes inseridos/atualizados.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir clientes_raw: {e}", exc_info=True)
        cur.close()
        conn.close()
        return pd.DataFrame()

    #Processa vendas
    vendas_raw = df[['email', 'data_venda', 'valor_venda', 'status_pedido']]
    vendas_data = []

    for idx, row in vendas_raw.iterrows():
        email = row['email']
        id_cliente = None

        if pd.notna(email) and str(email).strip() != '':
            cur.execute("SELECT id_cliente_raw FROM clientes_raw WHERE email = %s", (email,))
            res = cur.fetchone()
            if res:
                id_cliente = res[0]
            else:
                logger.warning(f"Email {email} não encontrado em clientes_raw.")
        else:
            logger.warning(f"Linha {idx} tem email inválido ou vazio: '{email}'")

        flag_valid = id_cliente is not None
        vendas_data.append((
            id_cliente,
            row['data_venda'],
            row['valor_venda'],
            row['status_pedido'],
            flag_valid
        ))
        logger.debug(f"Venda {idx}: {vendas_data[-1]}")

    try:
        cur.executemany("""
            INSERT INTO vendas_raw (id_cliente_raw, data_venda, valor_venda, status_pedido, flag_valid)
            VALUES (%s, %s, %s, %s, %s)
        """, vendas_data)
        conn.commit()
        logger.info(f"{len(vendas_data)} vendas inseridas.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir vendas_raw: {e}", exc_info=True)
        cur.close()
        conn.close()
        return pd.DataFrame()

    # Busca dados recentes para transformação (últimos 30 dias)
    try:
        cur.execute("""
            SELECT v.id_cliente_raw, c.nome, c.email, c.cidade, c.estado,
                   v.id_venda_raw, v.data_venda, v.valor_venda, v.status_pedido
            FROM vendas_raw v
            LEFT JOIN clientes_raw c ON v.id_cliente_raw = c.id_cliente_raw
            WHERE v.data_venda >= NOW() - INTERVAL '30 days'
        """)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df_new = pd.DataFrame(rows, columns=colnames)
        logger.info(f"{len(df_new)} registros carregados para transformação.")
    except Exception as e:
        logger.error(f"Erro ao buscar registros para transformação: {e}", exc_info=True)
        df_new = pd.DataFrame()
    finally:
        cur.close()
        conn.close()

    return df_new


#Função que será chamada pela DAG
def main(**kwargs):
    df = extract_data()
    os.makedirs('/opt/airflow/temp', exist_ok=True)
    output_path = '/opt/airflow/temp/df_extraido.pkl'
    df.to_pickle(output_path)
    logger.info(f"DataFrame salvo em {output_path}")
    return output_path  # Usado via XCom na DAG