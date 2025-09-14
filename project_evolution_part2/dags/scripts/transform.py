import os
import logging
import pandas as pd
from typing import Tuple, Dict, Any

#Configuração do logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Iniciando transformação de dados...")

    if df.empty:
        logger.error("O DataFrame de entrada está vazio. Não é possível continuar com a transformação.")
        raise ValueError("O DataFrame de entrada está vazio.")

    #Separar clientes (únicos)
    clientes = df[['id_cliente_raw', 'nome', 'email', 'cidade', 'estado']].drop_duplicates(subset=['id_cliente_raw']).copy()

    #Separar vendas
    vendas = df[['id_venda_raw', 'id_cliente_raw', 'data_venda', 'valor_venda', 'status_pedido']].copy()

    #Limpeza clientes
    clientes['nome'] = clientes['nome'].fillna('Nome Desconhecido').astype(str).str.strip().str.title()
    clientes['email'] = clientes['email'].fillna('sem_email@dominio.com')
    clientes['estado'] = clientes['estado'].fillna('desconhecido')

    #Limpeza vendas
    vendas['status_pedido'] = vendas['status_pedido'].fillna('desconhecido')
    vendas['venda_incompleta'] = vendas['valor_venda'].isnull()
    vendas['data_venda'] = pd.to_datetime(vendas['data_venda'], errors='coerce')
    vendas['data_venda_invalida'] = vendas['data_venda'].isna()
    vendas = vendas[vendas['id_cliente_raw'].notna()]

    #Mapeamento de status
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
    return clientes, vendas

def main(ti: Any) -> Dict[str, str]:
    try:
        #Recebe o caminho do arquivo da task anterior
        input_path = ti.xcom_pull(task_ids='extract_data')

        if not input_path or not isinstance(input_path, str) or not input_path.strip():
            logger.error("Caminho de arquivo inválido ou não fornecido pela tarefa 'extract_data'.")
            raise ValueError("Caminho de arquivo inválido ou não fornecido.")

        logger.info(f"Lendo DataFrame do arquivo: {input_path}")
        df = pd.read_pickle(input_path)

        if df.empty:
            logger.error(f"O DataFrame carregado do arquivo {input_path} está vazio.")
            raise ValueError(f"O DataFrame carregado do arquivo {input_path} está vazio.")

    except Exception as e:
        logger.error(f"Erro ao carregar arquivo pickle ou processar dados na tarefa 'transform_data': {e}")
        raise

    # Realiza a transformação dos dados
    try:
        clientes, vendas = transform_data(df)
    except Exception as e:
        logger.error(f"Erro na transformação de dados: {e}")
        raise

    # Criação de diretório temporário para salvar os resultados
    try:
        os.makedirs('/opt/airflow/temp', exist_ok=True)
        clientes_path = '/opt/airflow/temp/clientes.pkl'
        vendas_path = '/opt/airflow/temp/vendas.pkl'

        #Salvando os DataFrames transformados
        clientes.to_pickle(clientes_path)
        vendas.to_pickle(vendas_path)

        logger.info("DataFrames transformados salvos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar os DataFrames transformados: {e}")
        raise

    #Retorno com caminhos dos arquivos para a próxima task
    return {'clientes': clientes_path, 'vendas': vendas_path}