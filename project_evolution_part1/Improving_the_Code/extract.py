#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
from dotenv import load_dotenv
from datetime import datetime


# In[29]:


import os
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Configuração do logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Carregando o ambiente
load_dotenv('../../.env')
senha_db = os.getenv("DB_PASSWORD")

if not senha_db:
    raise ValueError("Senha não encontrada, verifique o arquivo .env")

csv_path = '../../csv/novas_vendas.csv'

def extract_data():
    logger.info("Iniciando a extração de dados")

    # Lendo o CSV
    if not os.path.exists(csv_path):
        logger.info("Arquivo não encontrado. Extração interrompida!")
        return pd.DataFrame()

    df = pd.read_csv(csv_path, encoding='latin1')
    logger.info(f"df carregado com {len(df)} linhas.")

    try:
        conn = psycopg2.connect(
            host='localhost',
            database='vendas_db',
            user='postgres',
            password=senha_db
        )
        cur = conn.cursor()
    except Exception as e:
        return pd.DataFrame()

    # =======================
    # Inserção em clientes_raw
    # =======================
    clientes_raw = df[['nome', 'email', 'cidade', 'estado']].drop_duplicates()

    # Marca clientes inválidos (sem e-mail) com flag = FALSE
    clientes_raw['flag_valid'] = clientes_raw['email'].apply(
        lambda x: False if pd.isna(x) or str(x).strip().lower() in ['nan', ''] else True
    )

    clientes_data = [
        (row['nome'], row['email'], row['cidade'], row['estado'], row['flag_valid'])
        for _, row in clientes_raw.iterrows()
    ]

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
        logger.info(f"{len(clientes_data)} clientes inseridos/atualizados em clientes_raw.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir clientes_raw: {e}")
        cur.close()
        conn.close()
        return pd.DataFrame()

    # =====================
    # Inserção em vendas_raw
    # =====================
    vendas_raw = df[['email', 'data_venda', 'valor_venda', 'status_pedido']]
    vendas_data = []

    try:
        for _, row in vendas_raw.iterrows():
            email = row['email']
            id_cliente = None

            if pd.notna(email):
                cur.execute("SELECT id_cliente_raw FROM clientes_raw WHERE email = %s", (email,))
                res = cur.fetchone()
                if res:
                    id_cliente = res[0]
                    
            flag_valid = True if id_cliente is not None else False

            vendas_data.append((
                id_cliente,
                row['data_venda'],
                row['valor_venda'],
                row['status_pedido'],
                flag_valid
            ))

        cur.executemany("""
            INSERT INTO vendas_raw (id_cliente_raw, data_venda, valor_venda, status_pedido, flag_valid)
            VALUES (%s, %s, %s, %s, %s)
        """, vendas_data)

        conn.commit()
        logger.info(f"{len(vendas_data)} vendas inseridas/atualizadas em vendas_raw.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir vendas_raw: {e}")
        cur.close()
        conn.close()
        return pd.DataFrame()

    # =============================
    # Carrega apenas os últimos 7 dias
    # =============================
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
        logger.info(f"{len(df_new)} registros novos retornados para transformação.")
    except Exception as e:
        logger.error(f"Erro ao buscar registros novos: {e}")
        df_new = pd.DataFrame()
    finally:
        cur.close()
        conn.close()

    return df_new


if __name__ == "__main__":
    df_new = extract_data()
    print(df_new.head())




