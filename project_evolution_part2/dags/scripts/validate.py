import os
import logging
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('/opt/airflow/.env')  # Se precisar usar variáveis de ambiente

def validate_data(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    logger.info("Iniciando validação dos dados...")

    if df.empty:
        logger.error("DataFrame de entrada está vazio!")
        raise ValueError("DataFrame de entrada vazio")

    # CLIENTES
    clientes = df[['id_cliente_raw', 'nome', 'email', 'cidade', 'estado']].drop_duplicates(subset=['id_cliente_raw']).copy()

    # Limpeza básica
    clientes['email'] = clientes['email'].astype(str).str.strip().str.lower()
    clientes['estado'] = clientes['estado'].astype(str).str.strip().str[:2]
    clientes['nome'] = clientes['nome'].fillna('').astype(str).str.strip().str.title()
    clientes['cidade'] = clientes['cidade'].fillna('Desconhecido').astype(str).str.strip()

    #Validação
    clientes['email_valido'] = clientes['email'].apply(lambda x: isinstance(x, str) and '@' in x and x not in ['', 'nan'])
    clientes['nome_valido'] = clientes['nome'].apply(lambda x: isinstance(x, str) and len(x.strip()) > 0)

    clientes_validos = clientes[clientes['email_valido'] & clientes['nome_valido']].copy()
    clientes_invalidos = clientes[~(clientes['email_valido'] & clientes['nome_valido'])]

    if not clientes_invalidos.empty:
        logger.warning(f"{len(clientes_invalidos)} clientes inválidos serão descartados:")
        for idx, row in clientes_invalidos.iterrows():
            logger.warning(f"id_cliente_raw={row['id_cliente_raw']}, nome='{row['nome']}', email='{row['email']}'")

    clientes_validos.rename(columns={'id_cliente_raw': 'id_cliente'}, inplace=True)
    clientes_validos = clientes_validos[['id_cliente', 'nome', 'email', 'cidade', 'estado']]

    # VENDAS
    vendas = df[['id_venda_raw', 'id_cliente_raw', 'email', 'data_venda', 'valor_venda', 'status_pedido']].copy()

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
        .astype(str)
        .str.strip()
        .str.lower()
        .map(status_map)
    )

    #Filtrar apenas status válidos (não nulos após o mapeamento)
    vendas = vendas[vendas['status_pedido'].notna()]

    vendas['data_venda'] = pd.to_datetime(vendas['data_venda'], errors='coerce')
    vendas['valor_venda_valido'] = vendas['valor_venda'].apply(lambda x: pd.notna(x) and x >= 0)

    clientes_validos_ids = set(clientes_validos['id_cliente'])
    vendas['cliente_valido'] = vendas['id_cliente_raw'].apply(lambda x: x in clientes_validos_ids)

    vendas_validas = vendas[
        vendas['data_venda'].notna() &
        vendas['valor_venda_valido'] &
        vendas['cliente_valido']
    ].copy()

    vendas_invalidas = vendas[~(
        vendas['data_venda'].notna() &
        vendas['valor_venda_valido'] &
        vendas['cliente_valido']
    )]

    if not vendas_invalidas.empty:
        logger.warning(f"{len(vendas_invalidas)} vendas inválidas foram descartadas:")
        cols_log = [col for col in ['id_venda_raw', 'id_cliente_raw', 'valor_venda', 'data_venda'] if col in vendas_invalidas.columns]
        logger.warning(f"\n{vendas_invalidas[cols_log]}")

    vendas_validas.rename(columns={'id_cliente_raw': 'id_cliente'}, inplace=True)
    vendas_validas = vendas_validas[['id_venda_raw', 'id_cliente', 'email', 'data_venda', 'valor_venda', 'status_pedido']]

    return clientes_validos, vendas_validas

def main(ti):
    logger.info("Executando validação a partir de arquivo extraído...")

    input_path = ti.xcom_pull(task_ids='extract_data')
    if not input_path or not isinstance(input_path, str) or not input_path.strip():
        logger.error("Caminho do arquivo extraído inválido ou não informado via XCom")
        raise ValueError("Arquivo extraído inválido")

    df = pd.read_pickle(input_path)

    clientes_validados, vendas_validadas = validate_data(df)

    os.makedirs('/opt/airflow/temp', exist_ok=True)
    clientes_path = '/opt/airflow/temp/clientes_validos.pkl'
    vendas_path = '/opt/airflow/temp/vendas_validas.pkl'

    clientes_validados.to_pickle(clientes_path)
    vendas_validadas.to_pickle(vendas_path)

    logger.info(f"{len(clientes_validados)} clientes válidos salvos em {clientes_path}")
    logger.info(f"{len(vendas_validadas)} vendas válidas salvas em {vendas_path}")

    return {'clientes': clientes_path, 'vendas': vendas_path}
