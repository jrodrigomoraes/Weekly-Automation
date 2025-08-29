# send_report.py

def main():
    import smtplib
    from email.message import EmailMessage
    import mimetypes
    import os
    from dotenv import load_dotenv

    load_dotenv()

    remetente = os.getenv('REMETENTE_EMAIL')
    destinatario = os.getenv('DESTINATARIO_EMAIL')
    senha = os.getenv('SENHA_EMAIL')

    assunto = 'Envio de Relatórios de Clientes e Atrasados'
    mensagem = """
    Segue em anexo o relatório semanal com CSVs e o PDF com gráficos.

    Atenciosamente,
    José Rodrigo
    """

    anexos = [
        './relatorio_ecommerce.pdf',
        './pedidos_atrasados.csv',
        './top_10_clientes.csv'
    ]

    msg = EmailMessage()
    msg['From'] = remetente
    msg['To'] = destinatario
    msg['Subject'] = assunto
    msg.set_content(mensagem)

    for anexo in anexos:
        mime_type, _ = mimetypes.guess_type(anexo)
        if mime_type:
            mime_type, mime_subtype = mime_type.split('/')
            with open(anexo, 'rb') as arquivo:
                msg.add_attachment(arquivo.read(), maintype=mime_type, subtype=mime_subtype, filename=os.path.basename(anexo))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as email:
        email.login(remetente, senha)
        email.send_message(msg)

    print('Email enviado com sucesso!')

if __name__ == "__main__":
    main()