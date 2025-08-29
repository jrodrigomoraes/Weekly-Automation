#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Carregando variáveis de ambiente
load_dotenv('../../.env')
senha_db = os.getenv("DB_PASSWORD")

def load_banco(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    logger.info("Iniciando a carga no banco de dados")

    # Conectar ao banco
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='vendas_db',
            user='postgres',
            password=senha_db
        )
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco: {str(e)}")
        return

    # Inserção na tabela `clientes`
    
    clientes = df1[['nome', 'email', 'cidade', 'estado']]
    clientes_data = [
        (row['nome'], row['email'], row['cidade'], row['estado'])
        for _, row in clientes.iterrows()
    ]

    try:
        cur.executemany("""
            INSERT INTO clientes (nome, email, cidade, estado)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, clientes_data)
        conn.commit()
        logger.info(f"{len(clientes_data)} clientes inseridos/atualizados em clientes.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir clientes: {e}")
        cur.close()
        conn.close()
        return

    # Mapeia id_cliente com base no e-mail
    try:
        cur.execute("SELECT id_cliente, email FROM clientes")
        rows = cur.fetchall()
        id_map = {email: id_cliente for id_cliente, email in rows}
        logger.info("Mapeamento de email → id_cliente realizado.")
    except Exception as e:
        logger.error(f"Erro ao mapear IDs dos clientes: {e}")
        cur.close()
        conn.close()
        return

    #Substituir id_cliente_raw pelo id_cliente real
    if 'email' not in df2.columns:
        logger.error("Coluna 'email' ausente no DataFrame de vendas. Não é possível mapear o id_cliente.")
        cur.close()
        conn.close()
        return

    df2['id_cliente'] = df2['email'].map(id_map)
    df2 = df2[df2['id_cliente'].notna()]

    # Inserção na tabela vendas
    vendas = df2[['id_cliente', 'data_venda', 'valor_venda', 'status_pedido']]
    vendas_data = [
        (row['id_cliente'], row['data_venda'], row['valor_venda'], row['status_pedido'])
        for _, row in vendas.iterrows()
    ]

    try:
        cur.executemany("""
            INSERT INTO vendas (id_cliente, data_venda, valor_venda, status_pedido)
            VALUES (%s, %s, %s, %s)
        """, vendas_data)
        conn.commit()
        logger.info(f"{len(vendas_data)} vendas inseridas em vendas.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir vendas: {e}")
    finally:
        cur.close()
        conn.close()


# In[ ]:




