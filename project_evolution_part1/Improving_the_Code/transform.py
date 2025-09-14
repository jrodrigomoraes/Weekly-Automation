import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame):
    logger.info("Iniciando transformação de dados...")

    # Separar clientes (únicos)
    clientes = df[['id_cliente_raw', 'nome', 'email', 'cidade', 'estado']].drop_duplicates(subset=['id_cliente_raw']).copy()

    # Separar vendas
    vendas = df[['id_venda_raw', 'id_cliente_raw', 'data_venda', 'valor_venda', 'status_pedido']].copy()

    # Limpeza clientes
    clientes['nome'] = clientes['nome'].fillna('Nome Desconhecido').astype(str).str.strip().str.title()
    clientes['email'] = clientes['email'].fillna('sem_email@dominio.com')
    clientes['estado'] = clientes['estado'].fillna('desconhecido')

    # Limpeza vendas
    vendas['status_pedido'] = vendas['status_pedido'].fillna('desconhecido')
    vendas['venda_incompleta'] = vendas['valor_venda'].isnull()
    vendas['data_venda'] = pd.to_datetime(vendas['data_venda'], errors='coerce')
    vendas['data_venda_invalida'] = vendas['data_venda'].isna()
    vendas = vendas[vendas['id_cliente_raw'].notna()]

    status_map = {
        "concluído": "entregue",
        "finalizado": "entregue",
        "pendente": "em transporte",
        "em trânsito": "em transporte",
        "encaminhado": "em transporte",
        "atrasado": "atrasado"
    }
    vendas['status_pedido'] = (
        vendas['status_pedido']
        .astype(str).str.strip().str.lower()
        .map(status_map)
        .fillna('desconhecido')
    )

    logger.info("Transformação concluída com sucesso.")

    # Retorna os DataFrames transformados
    return clientes, vendas