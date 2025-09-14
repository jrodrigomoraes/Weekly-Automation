import os
import logging
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv


def run_analysis(temp_dir="/opt/airflow/temp", db_host="postgres", db_name="vendas_db", db_user="airflow", db_password=None):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    if db_password is None:
        load_dotenv('/opt/airflow/.env')
        db_password = os.getenv("DB_PASSWORD")

    if not db_password:
        raise ValueError("Senha do banco de dados não foi definida no arquivo .env ou parâmetro")

    def get_top_10_clientes(conn, dias=7):
        query = f"""
            SELECT 
                c.id_cliente, 
                c.nome, 
                SUM(v.valor_venda) as total_vendas, 
                COUNT(v.id_venda) as qtd_compras
            FROM clientes c
            JOIN vendas v ON c.id_cliente = v.id_cliente
            WHERE v.data_venda >= CURRENT_DATE - INTERVAL '{dias} days'
            AND v.status_pedido = 'entregue'
            GROUP BY c.id_cliente, c.nome
            ORDER BY total_vendas DESC
            LIMIT 10;
        """
        return pd.read_sql(query, conn)

    def atraso_clientes(conn, dias=30):
        query = f"""
            SELECT
                c.id_cliente,
                c.nome,
                v.id_venda,
                v.data_venda,
                c.email,
                c.estado,
                v.status_pedido
            FROM clientes c
            JOIN vendas v ON c.id_cliente = v.id_cliente
            WHERE v.status_pedido = 'atrasado'
            AND v.data_venda >= DATE_TRUNC('day', CURRENT_DATE) - INTERVAL '{dias} days'
            ORDER BY c.id_cliente, v.data_venda DESC;
        """
        return pd.read_sql(query, conn)

    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        logger.info("Conexão bem-sucedida com o banco.")

        top_10 = get_top_10_clientes(conn, dias=7)
        atrasados = atraso_clientes(conn, dias=30)

        # Salvar CSVs
        csv_top_10 = os.path.join(temp_dir, 'top_10_clientes.csv')
        csv_atrasados = os.path.join(temp_dir, 'pedidos_atrasados.csv')
        top_10.to_csv(csv_top_10, index=False)
        atrasados.to_csv(csv_atrasados, index=False)
        logger.info(f"CSV salvos: {csv_top_10}, {csv_atrasados}")

        # Gráfico 1: Top 10 clientes por receita
        plt.figure(figsize=(10, 6))
        sns.barplot(x='total_vendas', y='nome', data=top_10, palette="Blues_d")
        plt.title("Top 10 Clientes por Receita")
        plt.tight_layout()
        graf1_path = os.path.join(temp_dir, "grafico_top10_receita.png")
        plt.savefig(graf1_path)
        plt.close()

        #Gráfico 2: Top 10 clientes com mais atrasos
        atrasos_por_cliente = atrasados['nome'].value_counts().head(10).reset_index()
        atrasos_por_cliente.columns = ['nome', 'qtd_atrasos']

        plt.figure(figsize=(10, 6))
        sns.barplot(x='qtd_atrasos', y='nome', data=atrasos_por_cliente, palette="Reds_d")
        plt.title("Top 10 Clientes com Mais Atrasos")
        plt.tight_layout()
        graf2_path = os.path.join(temp_dir, "grafico_top10_atrasos.png")
        plt.savefig(graf2_path)
        plt.close()

        #Gráfico 3: Distribuição de atrasos por estado
        atrasos_por_estado = atrasados['estado'].value_counts().reset_index()
        atrasos_por_estado.columns = ['estado', 'qtd_atrasos']

        plt.figure(figsize=(10, 6))
        sns.barplot(x='estado', y='qtd_atrasos', data=atrasos_por_estado, palette="Oranges_d")
        plt.title("Distribuição de Atrasos por Estado")
        plt.tight_layout()
        graf3_path = os.path.join(temp_dir, "grafico_atrasos_estado.png")
        plt.savefig(graf3_path)
        plt.close()

        # Gráfico 4: Evolução diária de atrasos
        atrasados['data_venda'] = pd.to_datetime(atrasados['data_venda'])
        evolucao_diaria = atrasados.groupby('data_venda')['id_cliente'].count().reset_index()

        plt.figure(figsize=(12, 6))
        sns.lineplot(x='data_venda', y='id_cliente', data=evolucao_diaria, marker='o', linewidth=2, color='dodgerblue')
        plt.title('Evolução Diária dos Atrasos (Últimos 30 dias)')
        plt.xlabel('Data')
        plt.ylabel('Total de Atrasos')
        plt.xticks(rotation=45)
        plt.tight_layout()
        graf4_path = os.path.join(temp_dir, "grafico_evolucao_atrasos.png")
        plt.savefig(graf4_path)
        plt.close()

        logger.info(f"Gráficos salvos em: {temp_dir}")

        # Retorna os paths gerados para facilitar downstream
        return {
            "csv_top_10": csv_top_10,
            "csv_atrasados": csv_atrasados,
            "grafico_top10_receita": graf1_path,
            "grafico_top10_atrasos": graf2_path,
            "grafico_atrasos_estado": graf3_path,
            "grafico_evolucao_atrasos": graf4_path,
        }

    except Exception as e:
        logger.error(f"Erro na execução da análise: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Conexão com o banco encerrada.")


if __name__ == "__main__":
    run_analysis()