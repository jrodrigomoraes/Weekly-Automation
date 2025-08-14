import os
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()
senha = os.getenv("DB_PASSWORD")

if not senha:
    raise ValueError("Senha do banco de dados não foi definida no arquivo .env")

# Funções para consultas
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

# Conexão e execução
try:
    conn = psycopg2.connect(
        host="localhost",
        database="vendas_db",
        user="postgres",
        password=senha
    )

    print("Conexão bem-sucedida com o banco.")

    top_10 = get_top_10_clientes(conn, dias=7)
    atrasados = atraso_clientes(conn, dias=30)

    # Salvar CSVs
    top_10.to_csv('top_10_clientes.csv', index=False)
    atrasados.to_csv('pedidos_atrasados.csv', index=False)

    # ===== Gráfico 1: Top 10 clientes por receita =====
    plt.figure(figsize=(10, 6))
    sns.barplot(x='total_vendas', y='nome', data=top_10, palette="Blues_d")
    plt.title("Top 10 Clientes por Receita")
    plt.tight_layout()
    plt.savefig("grafico_top10_receita.png")
    plt.close()

    # ===== Gráfico 2: Top 10 clientes com mais atrasos =====
    atrasos_por_cliente = atrasados['nome'].value_counts().head(10).reset_index()
    atrasos_por_cliente.columns = ['nome', 'qtd_atrasos']

    plt.figure(figsize=(10, 6))
    sns.barplot(x='qtd_atrasos', y='nome', data=atrasos_por_cliente, palette="Reds_d")
    plt.title("Top 10 Clientes com Mais Atrasos")
    plt.tight_layout()
    plt.savefig("grafico_top10_atrasos.png")
    plt.close()

    # ===== Gráfico 3: Distribuição de atrasos por estado =====
    atrasos_por_estado = atrasados['estado'].value_counts().reset_index()
    atrasos_por_estado.columns = ['estado', 'qtd_atrasos']

    plt.figure(figsize=(10, 6))
    sns.barplot(x='estado', y='qtd_atrasos', data=atrasos_por_estado, palette="Oranges_d")
    plt.title("Distribuição de Atrasos por Estado")
    plt.tight_layout()
    plt.savefig("grafico_atrasos_estado.png")
    plt.close()

    # ===== Gráfico 4: Evolução diária de atrasos =====
    atrasados['data_venda'] = pd.to_datetime(atrasados['data_venda'])
    evolucao_diaria = atrasados.groupby('data_venda')['id_cliente'].count().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(x='data_venda', y='id_cliente', data=evolucao_diaria, marker='o', linewidth=2, color='dodgerblue')
    plt.title('Evolução Diária dos Atrasos (Últimos 30 dias)')
    plt.xlabel('Data')
    plt.ylabel('Total de Atrasos')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("grafico_evolucao_atrasos.png")
    plt.close()

except Exception as e:
    print(f"Erro: {e}")
finally:
    if conn:
        conn.close()
