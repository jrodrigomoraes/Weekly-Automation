def main():
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet

    pdf_file = "relatorio_ecommerce.pdf"
    doc = SimpleDocTemplate(pdf_file, pagesize=A4)
    styles = getSampleStyleSheet()
    flow = []

    flow.append(Paragraph("<b>Relatório de Vendas e Atrasos</b>", styles["Title"]))
    flow.append(Spacer(1, 20))

    graficos = [
        ("Top 10 Clientes por Receita", "grafico_top10_receita.png"),
        ("Top 10 Clientes com Mais Atrasos", "grafico_top10_atrasos.png"),
        ("Distribuição de Atrasos por Estado", "grafico_atrasos_estado.png"),
        ("Evolução Diária dos Atrasos", "grafico_evolucao_atrasos.png")
    ]

    for titulo, img_path in graficos:
        flow.append(Paragraph(f"<b>{titulo}</b>", styles["Heading2"]))
        flow.append(Image(img_path, width=400, height=250))
        flow.append(Spacer(1, 20))

    doc.build(flow)
    print(f"PDF gerado: {pdf_file}")

if __name__ == "__main__":
    main()