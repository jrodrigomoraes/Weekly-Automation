import os
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import date

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('/opt/airflow/.env')
senha_db = os.getenv("DB_PASSWORD")

def load_banco(df_clientes: pd.DataFrame, df_vendas: pd.DataFrame) -> None:
    logger.info("Iniciando a carga no banco de dados")

    try:
        conn = psycopg2.connect(
            host='postgres',
            database='vendas_db',
            user='airflow',
            password=senha_db
        )
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco: {e}")
        raise

    # CARGA DE CLIENTES
    clientes = df_clientes[['nome', 'email', 'cidade', 'estado']]
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
        logger.info(f"{len(clientes_data)} clientes inseridos/atualizados.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao inserir clientes: {e}")
        cur.close()
        conn.close()
        raise

    #Mapeamento email → id_cliente
    try:
        cur.execute("SELECT id_cliente, email FROM clientes")
        rows = cur.fetchall()
        id_map = {email: id_cliente for id_cliente, email in rows}
        logger.info("Mapeamento email → id_cliente realizado.")
    except Exception as e:
        logger.error(f"Erro ao mapear IDs: {e}")
        cur.close()
        conn.close()
        raise

    # CARGA DE VENDAS (SCD2)
    df_vendas['id_cliente'] = df_vendas['email'].map(id_map)
    df_vendas = df_vendas[df_vendas['id_cliente'].notna()]

    for _, row in df_vendas.iterrows():
        id_venda_raw = str(row.get('id_venda_raw')).strip()  # <- CONVERSÃO IMPORTANTE
        id_cliente = int(row['id_cliente'])
        data_venda = row['data_venda']
        valor_venda = row['valor_venda']
        status_pedido = row['status_pedido']
        valid_from = date.today()

        # Verifica se já existe versão atual da venda
        cur.execute("""
            SELECT status_pedido FROM vendas
            WHERE id_venda_raw = %s AND is_current = TRUE
        """, (id_venda_raw,))
        result = cur.fetchone()

        if result is None:
            # Nova venda
            cur.execute("""
                INSERT INTO vendas (id_cliente, data_venda, valor_venda, status_pedido, 
                                    id_venda_raw, valid_from, valid_to, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE)
            """, (id_cliente, data_venda, valor_venda, status_pedido, id_venda_raw, valid_from))
            logger.info(f"Venda nova inserida: {id_venda_raw}")
        elif result[0] != status_pedido:
            # Atualiza versão antiga e insere nova
            cur.execute("""
                UPDATE vendas
                SET valid_to = %s, is_current = FALSE
                WHERE id_venda_raw = %s AND is_current = TRUE
            """, (valid_from, id_venda_raw))
            cur.execute("""
                INSERT INTO vendas (id_cliente, data_venda, valor_venda, status_pedido, 
                                    id_venda_raw, valid_from, valid_to, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE)
            """, (id_cliente, data_venda, valor_venda, status_pedido, id_venda_raw, valid_from))
            logger.info(f"Venda atualizada (novo status): {id_venda_raw}")
        else:
            logger.info(f"Venda já existente e atual: {id_venda_raw} (sem mudanças)")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Carga de vendas finalizada.")

def main(ti):
    clientes_path = ti.xcom_pull(task_ids='validate_data')['clientes']
    vendas_path = ti.xcom_pull(task_ids='validate_data')['vendas']

    clientes = pd.read_pickle(clientes_path)
    vendas = pd.read_pickle(vendas_path)

    load_banco(clientes, vendas)
    logger.info("Carga concluída com sucesso.")