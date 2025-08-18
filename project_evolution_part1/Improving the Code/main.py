import logging
from extract import extract_data
from transform import transform_data
from validate import validate_data
from load import load_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logger.info("Iniciando pipeline ETL...")
    
    clientes, vendas = extract_data()
    logger.info("Extração finalizada.")
    
    clientes, vendas = transform_data(clientes, vendas)
    logger.info("Transformação finalizada.")
    
    valid = validate_data(clientes, vendas)
    if not valid:
        logger.error("Validação falhou. Abortando pipeline.")
        return
    
    load_data(clientes, vendas)
    logger.info("Carregamento finalizado.")
    
    logger.info("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()