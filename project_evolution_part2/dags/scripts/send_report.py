import os
import logging
import mimetypes
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

def send_report(temp_dir="/opt/airflow/temp"):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    load_dotenv('/opt/airflow/.env')

    remetente = os.getenv('REMETENTE_EMAIL')
    destinatario = os.getenv('DESTINATARIO_EMAIL')
    senha = os.getenv('SENHA_EMAIL')

    if not all([remetente, destinatario, senha]):
        logger.error("Variáveis de ambiente de e-mail não configuradas corretamente.")
        return

    assunto = 'Envio de Relatórios de Clientes e Atrasados'
    mensagem = """
    Segue em anexo o relatório semanal com CSVs e o PDF com gráficos.

    Atenciosamente,
    José Rodrigo
    """

    anexos = [
        os.path.join(temp_dir, 'relatorio_ecommerce.pdf'),
        os.path.join(temp_dir, 'pedidos_atrasados.csv'),
        os.path.join(temp_dir, 'top_10_clientes.csv')
    ]

    msg = EmailMessage()
    msg['From'] = remetente
    msg['To'] = destinatario
    msg['Subject'] = assunto
    msg.set_content(mensagem)

    for anexo in anexos:
        if not os.path.exists(anexo):
            logger.warning(f"Arquivo para anexo não encontrado: {anexo}. Pulando.")
            continue
        mime_type, _ = mimetypes.guess_type(anexo)
        if mime_type:
            mime_main, mime_sub = mime_type.split('/')
            with open(anexo, 'rb') as arquivo:
                msg.add_attachment(arquivo.read(), maintype=mime_main, subtype=mime_sub, filename=os.path.basename(anexo))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as email:
            email.login(remetente, senha)
            email.send_message(msg)
        logger.info('Email enviado com sucesso!')
    except Exception as e:
        logger.error(f"Erro ao enviar email: {e}")

if __name__ == "__main__":
    send_report()