#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Importando bibliotecas
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime


# In[3]:


#Carregando variáveis de ambiente
load_dotenv()
senha_db = os.getenv("DB_PASSWORD")

if not senha_db:
    raise ValueError("Senha do banco não encontrada. Verifique o .env")


# In[6]:


import os
import psycopg2
import pandas as pd
from datetime import datetime

# Conectando com o banco de dados
try:
    conn = psycopg2.connect(
        host='localhost',
        database='vendas_db',
        user='postgres',
        password=senha_db  # Certifique-se de definir a variável senha_db corretamente
    )
    cur = conn.cursor()
except Exception as e:
    raise ConnectionError(f"Erro ao conectar ao banco: {str(e)}")

# Atualizando no banco
csv_path = './novas_vendas.csv'

try:
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)

        colunas_esperadas = ['id_cliente', 'nome', 'email', 'cidade', 'estado', 
                             'id_venda', 'data_venda', 'valor_venda', 'status_pedido']

        if not all(col in df.columns for col in colunas_esperadas):
            raise ValueError("O CSV não contém todas as colunas esperadas.")

        # Dados de clientes
        dados_clientes = [
            (row['id_cliente'], row['nome'], row['email'], row['cidade'], row['estado'])
            for _, row in df.iterrows()
        ]

        cur.executemany("""
            INSERT INTO clientes (id_cliente, nome, email, cidade, estado)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_cliente) DO NOTHING
        """, dados_clientes)

        # Dados de vendas
        dados_venda = [
            (row['id_venda'], row['id_cliente'], row['data_venda'], row['valor_venda'], row['status_pedido'])
            for _, row in df.iterrows()
        ]

        cur.executemany("""
            INSERT INTO vendas (id_venda, id_cliente, data_venda, valor_venda, status_pedido)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_venda) DO NOTHING
        """, dados_venda)

        # Atualiza o banco
        conn.commit()

        # Formatar a data atual
        data = datetime.now()
        data_formatada = data.strftime("%d-%m-%y")
        print(f'Banco atualizado com sucesso no dia {data_formatada}')
        #Informando valores que foram inseridos
        print(f"{len(dados_clientes)} clientes processados e {len(dados_venda)} vendas processadas.")

    else:
        #Se o arquivo não existir
        print("O arquivo 'novas_vendas.csv' não foi encontrado. Continuando sem atualizar o banco.")

except Exception as e:
    conn.rollback()
    print(f"Erro ao atualizar banco: {str(e)}")

finally:
    # Fechar a conexão de forma segura
    cur.close()
    conn.close()


# In[ ]:




