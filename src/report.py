from pathlib import Path
from datetime import datetime
from typing import List
from dataclasses import dataclass

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT

from .analysis import AnalysisResult
from .config import config


class ReportGenerator:

    def __init__(self):
        self.output_dir = config.paths.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.styles = getSampleStyleSheet()
        self._setup_styles()

    def _setup_styles(self):
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.HexColor('#1a365d')
        ))

        self.styles.add(ParagraphStyle(
            name='SectionTitle',
            parent=self.styles['Heading2'],
            fontSize=16,
            spaceBefore=20,
            spaceAfter=10,
            textColor=colors.HexColor('#2c5282')
        ))

        self.styles.add(ParagraphStyle(
            name='Insight',
            parent=self.styles['Normal'],
            fontSize=11,
            leftIndent=20,
            spaceBefore=5,
            bulletIndent=10,
            textColor=colors.HexColor('#2d3748')
        ))

        self.styles.add(ParagraphStyle(
            name='Footer',
            parent=self.styles['Normal'],
            fontSize=9,
            alignment=TA_CENTER,
            textColor=colors.gray
        ))

    def generate(
            self,
            analyses: List[AnalysisResult],
            summary_stats: dict,
            filename: str = None
    ) -> Path:
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raport_gus_{timestamp}.pdf"

        filepath = self.output_dir / filename

        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=A4,
            rightMargin=2 * cm,
            leftMargin=2 * cm,
            topMargin=2 * cm,
            bottomMargin=2 * cm
        )

        story = []

        story.extend(self._build_title_page(summary_stats))
        story.append(PageBreak())

        story.extend(self._build_summary_section(summary_stats))
        story.append(PageBreak())

        for analysis in analyses:
            story.extend(self._build_analysis_section(analysis))
            story.append(PageBreak())

        story.extend(self._build_footer())

        doc.build(story)

        return filepath

    def _build_title_page(self, stats: dict) -> List:
        elements = []

        elements.append(Spacer(1, 3 * cm))

        elements.append(Paragraph(
            "Raport Analityczny",
            self.styles['CustomTitle']
        ))

        elements.append(Paragraph(
            "Koszty Utrzymania Zasobów Mieszkaniowych",
            self.styles['SectionTitle']
        ))

        elements.append(Spacer(1, 1 * cm))

        elements.append(Paragraph(
            f"Dane: GUS BDL (P3961)",
            self.styles['Normal']
        ))

        years = stats.get('years', [])
        if years:
            elements.append(Paragraph(
                f"Okres: {min(years)} - {max(years)}",
                self.styles['Normal']
            ))

        elements.append(Spacer(1, 2 * cm))

        elements.append(Paragraph(
            f"Wygenerowano: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            self.styles['Footer']
        ))

        return elements

    def _build_summary_section(self, stats: dict) -> List:
        elements = []

        elements.append(Paragraph("Podsumowanie danych", self.styles['SectionTitle']))
        elements.append(Spacer(1, 0.5 * cm))

        data = [
            ["Metryka", "Wartość"],
            ["Liczba rekordów", f"{stats.get('total_records', 0):,}"],
            ["Liczba województw", f"{stats.get('regions_count', 0)}"],
            ["Lata", ", ".join(map(str, stats.get('years', [])))],
            ["Kategorie", ", ".join(stats.get('categories', []))],
            ["Suma kosztów", f"{stats.get('total_value', 0) / 1000:,.1f} mln zł"],
            ["Średnia wartość", f"{stats.get('avg_value', 0):,.1f} tys. zł"],
        ]

        table = Table(data, colWidths=[8 * cm, 8 * cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c5282')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f7fafc')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#2d3748')),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ]))

        elements.append(table)

        return elements

    def _build_analysis_section(self, analysis: AnalysisResult) -> List:
        elements = []

        elements.append(Paragraph(analysis.name, self.styles['SectionTitle']))
        elements.append(Spacer(1, 0.3 * cm))

        elements.append(Paragraph(analysis.description, self.styles['Normal']))
        elements.append(Spacer(1, 0.5 * cm))

        if analysis.chart_static and analysis.chart_static.exists():
            img = Image(str(analysis.chart_static), width=16 * cm, height=9 * cm)
            elements.append(img)
            elements.append(Spacer(1, 0.5 * cm))

        if analysis.insights:
            elements.append(Paragraph("Kluczowe wnioski:", self.styles['Heading3']))
            elements.append(Spacer(1, 0.2 * cm))

            for insight in analysis.insights:
                elements.append(Paragraph(f"• {insight}", self.styles['Insight']))

        return elements

    def _build_footer(self) -> List:
        elements = []

        elements.append(Spacer(1, 1 * cm))
        elements.append(Paragraph(
            "Raport wygenerowany automatycznie przez system GUS Analytics",
            self.styles['Footer']
        ))
        elements.append(Paragraph(
            "Dane źródłowe: Bank Danych Lokalnych GUS (bdl.stat.gov.pl)",
            self.styles['Footer']
        ))

        return elements


class HTMLReportGenerator:

    def __init__(self):
        self.output_dir = config.paths.output_dir

    def generate(
            self,
            analyses: List[AnalysisResult],
            summary_stats: dict,
            filename: str = None
    ) -> Path:
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raport_gus_{timestamp}.html"

        filepath = self.output_dir / filename

        html_parts = [self._header(), self._summary(summary_stats)]

        for analysis in analyses:
            html_parts.append(self._analysis_section(analysis))

        html_parts.append(self._footer())

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(html_parts))

        return filepath

    def _header(self) -> str:
        return """
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Raport GUS Analytics</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: 'Segoe UI', Tahoma, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #1a365d; text-align: center; }
        h2 { color: #2c5282; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; }
        .summary-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        .summary-table th { background: #2c5282; color: white; padding: 12px; text-align: left; }
        .summary-table td { padding: 10px; border-bottom: 1px solid #e2e8f0; }
        .insights { background: #f7fafc; padding: 15px; border-radius: 5px; margin: 15px 0; }
        .insights li { margin: 8px 0; color: #2d3748; }
        .chart-container { margin: 20px 0; }
        .footer { text-align: center; color: #718096; margin-top: 40px; font-size: 0.9em; }
    </style>
</head>
<body>
<div class="container">
    <h1>Raport Analityczny GUS</h1>
    <p style="text-align: center; color: #718096;">Koszty Utrzymania Zasobów Mieszkaniowych</p>
"""

    def _summary(self, stats: dict) -> str:
        years = stats.get('years', [])
        return f"""
    <h2>Podsumowanie danych</h2>
    <table class="summary-table">
        <tr><th>Metryka</th><th>Wartość</th></tr>
        <tr><td>Liczba rekordów</td><td>{stats.get('total_records', 0):,}</td></tr>
        <tr><td>Liczba województw</td><td>{stats.get('regions_count', 0)}</td></tr>
        <tr><td>Lata</td><td>{', '.join(map(str, years))}</td></tr>
        <tr><td>Suma kosztów</td><td>{stats.get('total_value', 0) / 1000:,.1f} mln zł</td></tr>
    </table>
"""

    def _analysis_section(self, analysis: AnalysisResult) -> str:
        insights_html = ""
        if analysis.insights:
            items = "".join([f"<li>{i}</li>" for i in analysis.insights])
            insights_html = f'<div class="insights"><strong>Kluczowe wnioski:</strong><ul>{items}</ul></div>'

        chart_html = ""
        if analysis.chart_interactive:
            chart_html = f'<div class="chart-container">{analysis.chart_interactive}</div>'

        return f"""
    <h2>{analysis.name}</h2>
    <p>{analysis.description}</p>
    {chart_html}
    {insights_html}
"""

    def _footer(self) -> str:
        return f"""
    <div class="footer">
        <p>Raport wygenerowany: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <p>Źródło danych: Bank Danych Lokalnych GUS</p>
    </div>
</div>
</body>
</html>
"""