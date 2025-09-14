import os
import logging
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet

def generate_pdf_report(temp_dir="/opt/airflow/temp", output_filename="relatorio_ecommerce.pdf"):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    pdf_file = os.path.join(temp_dir, output_filename)
    doc = SimpleDocTemplate(pdf_file, pagesize=A4)
    styles = getSampleStyleSheet()
    flow = []

    flow.append(Paragraph("<b>Relatório de Vendas e Atrasos</b>", styles["Title"]))
    flow.append(Spacer(1, 20))

    graficos = [
        ("Top 10 Clientes por Receita", os.path.join(temp_dir, "grafico_top10_receita.png")),
        ("Top 10 Clientes com Mais Atrasos", os.path.join(temp_dir, "grafico_top10_atrasos.png")),
        ("Distribuição de Atrasos por Estado", os.path.join(temp_dir, "grafico_atrasos_estado.png")),
        ("Evolução Diária dos Atrasos", os.path.join(temp_dir, "grafico_evolucao_atrasos.png"))
    ]

    for titulo, img_path in graficos:
        if not os.path.exists(img_path):
            logger.warning(f"Imagem não encontrada: {img_path}. Pulando.")
            continue
        flow.append(Paragraph(f"<b>{titulo}</b>", styles["Heading2"]))
        flow.append(Image(img_path, width=400, height=250))
        flow.append(Spacer(1, 20))

    doc.build(flow)
    logger.info(f"PDF gerado: {pdf_file}")
    return pdf_file


if __name__ == "__main__":
    generate_pdf_report()