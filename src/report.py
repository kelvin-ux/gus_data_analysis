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
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

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
            fontSize=10,
            leftIndent=20,
            spaceBefore=3,
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
            rightMargin=1.5 * cm,
            leftMargin=1.5 * cm,
            topMargin=1.5 * cm,
            bottomMargin=1.5 * cm
        )

        story = []

        story.extend(self._build_title_page(summary_stats))
        story.append(PageBreak())

        story.extend(self._build_summary_section(summary_stats))
        story.append(PageBreak())

        for i, analysis in enumerate(analyses):
            story.extend(self._build_analysis_section(analysis))
            if i < len(analyses) - 1:
                story.append(PageBreak())

        story.extend(self._build_footer())

        doc.build(story)

        return filepath

    def _build_title_page(self, stats: dict) -> List:
        elements = []

        elements.append(Spacer(1, 4 * cm))

        elements.append(Paragraph(
            "Raport Analityczny",
            self.styles['CustomTitle']
        ))

        elements.append(Spacer(1, 0.5 * cm))

        elements.append(Paragraph(
            "Koszty Utrzymania Zasobow Mieszkaniowych",
            self.styles['SectionTitle']
        ))

        elements.append(Spacer(1, 1 * cm))

        elements.append(Paragraph(
            "Dane: GUS Bank Danych Lokalnych (P3961)",
            self.styles['Normal']
        ))

        years = stats.get('years', [])
        if years:
            elements.append(Paragraph(
                f"Okres analizy: {min(years)} - {max(years)}",
                self.styles['Normal']
            ))

        elements.append(Paragraph(
            f"Liczba wojewodztw: {stats.get('regions_count', 0)}",
            self.styles['Normal']
        ))

        elements.append(Spacer(1, 3 * cm))

        elements.append(Paragraph(
            f"Wygenerowano: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            self.styles['Footer']
        ))

        elements.append(Paragraph(
            "System GUS Analytics - Automatyczna analiza danych",
            self.styles['Footer']
        ))

        return elements

    def _build_summary_section(self, stats: dict) -> List:
        elements = []

        elements.append(Paragraph("Podsumowanie danych", self.styles['SectionTitle']))
        elements.append(Spacer(1, 0.5 * cm))

        years = stats.get('years', [])
        years_str = f"{min(years)} - {max(years)}" if years else "brak"

        data = [
            ["Metryka", "Wartosc"],
            ["Liczba rekordow", f"{stats.get('total_records', 0):,}"],
            ["Liczba wojewodztw", f"{stats.get('regions_count', 0)}"],
            ["Zakres lat", years_str],
            ["Kategorie", ", ".join(stats.get('categories', []))],
            ["Suma kosztow", f"{stats.get('total_value', 0) / 1000:,.1f} mln zl"],
            ["Srednia wartosc", f"{stats.get('avg_value', 0):,.1f} tys. zl"],
            ["Wartosc minimalna", f"{stats.get('min_value', 0):,.1f} tys. zl"],
            ["Wartosc maksymalna", f"{stats.get('max_value', 0):,.1f} tys. zl"],
        ]

        table = Table(data, colWidths=[8 * cm, 8 * cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c5282')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f7fafc')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#2d3748')),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))

        elements.append(table)

        return elements

    def _build_analysis_section(self, analysis: AnalysisResult) -> List:
        elements = []

        elements.append(Paragraph(analysis.name, self.styles['SectionTitle']))
        elements.append(Spacer(1, 0.2 * cm))

        elements.append(Paragraph(analysis.description, self.styles['Normal']))
        elements.append(Spacer(1, 0.3 * cm))

        if analysis.chart_static and analysis.chart_static.exists():
            try:
                img = Image(str(analysis.chart_static), width=17 * cm, height=9 * cm)
                elements.append(img)
                elements.append(Spacer(1, 0.3 * cm))
            except Exception as e:
                elements.append(Paragraph(f"[Wykres niedostepny: {e}]", self.styles['Normal']))

        if analysis.insights:
            elements.append(Paragraph("Kluczowe wnioski:", self.styles['Heading3']))
            elements.append(Spacer(1, 0.1 * cm))

            for insight in analysis.insights[:8]:
                safe_insight = insight.encode('ascii', 'replace').decode('ascii')
                elements.append(Paragraph(f"* {safe_insight}", self.styles['Insight']))

        return elements

    def _build_footer(self) -> List:
        elements = []

        elements.append(Spacer(1, 1 * cm))
        elements.append(Paragraph(
            "Raport wygenerowany automatycznie przez system GUS Analytics",
            self.styles['Footer']
        ))
        elements.append(Paragraph(
            "Dane zrodlowe: Bank Danych Lokalnych GUS (bdl.stat.gov.pl)",
            self.styles['Footer']
        ))
        elements.append(Paragraph(
            f"Data generacji: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
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
        * { box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        .container { 
            max-width: 1400px; 
            margin: 0 auto; 
            background: white; 
            padding: 40px; 
            border-radius: 12px; 
            box-shadow: 0 10px 40px rgba(0,0,0,0.2); 
        }
        h1 { 
            color: #1a365d; 
            text-align: center; 
            margin-bottom: 10px;
            font-size: 2.5em;
        }
        .subtitle {
            text-align: center;
            color: #718096;
            margin-bottom: 30px;
            font-size: 1.1em;
        }
        h2 { 
            color: #2c5282; 
            border-bottom: 3px solid #4299e1; 
            padding-bottom: 10px;
            margin-top: 40px;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .summary-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }
        .summary-card .value {
            font-size: 1.8em;
            font-weight: bold;
        }
        .summary-card .label {
            font-size: 0.9em;
            opacity: 0.9;
        }
        .analysis-section {
            background: #f8fafc;
            padding: 25px;
            border-radius: 10px;
            margin: 25px 0;
            border-left: 4px solid #4299e1;
        }
        .insights { 
            background: #edf2f7; 
            padding: 20px; 
            border-radius: 8px; 
            margin: 15px 0; 
        }
        .insights h4 {
            margin-top: 0;
            color: #2d3748;
        }
        .insights ul {
            margin: 0;
            padding-left: 20px;
        }
        .insights li { 
            margin: 8px 0; 
            color: #4a5568;
            line-height: 1.5;
        }
        .chart-container { 
            margin: 20px 0; 
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }
        .footer { 
            text-align: center; 
            color: #718096; 
            margin-top: 50px; 
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
            font-size: 0.9em; 
        }
        .toc {
            background: #edf2f7;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }
        .toc h3 { margin-top: 0; }
        .toc ul { columns: 2; }
        .toc li { margin: 5px 0; }
        .toc a { color: #4299e1; text-decoration: none; }
        .toc a:hover { text-decoration: underline; }
    </style>
</head>
<body>
<div class="container">
    <h1>Raport Analityczny GUS</h1>
    <p class="subtitle">Koszty Utrzymania Zasobow Mieszkaniowych w Polsce</p>
"""

    def _summary(self, stats: dict) -> str:
        years = stats.get('years', [])
        years_str = f"{min(years)} - {max(years)}" if years else "brak"

        return f"""
    <div class="summary-grid">
        <div class="summary-card">
            <div class="value">{stats.get('total_records', 0):,}</div>
            <div class="label">Rekordow</div>
        </div>
        <div class="summary-card">
            <div class="value">{stats.get('regions_count', 0)}</div>
            <div class="label">Wojewodztw</div>
        </div>
        <div class="summary-card">
            <div class="value">{years_str}</div>
            <div class="label">Zakres lat</div>
        </div>
        <div class="summary-card">
            <div class="value">{stats.get('total_value', 0) / 1000:,.1f}</div>
            <div class="label">Suma kosztow (mln zl)</div>
        </div>
    </div>

    <div class="toc">
        <h3>Spis tresci</h3>
        <ul>
            <li><a href="#trends">Trendy czasowe</a></li>
            <li><a href="#regions">Analiza regionalna</a></li>
            <li><a href="#structure">Struktura kosztow</a></li>
            <li><a href="#dynamics">Dynamika zmian</a></li>
            <li><a href="#anomalies">Analiza anomalii</a></li>
            <li><a href="#ranking">Ranking wojewodztw</a></li>
            <li><a href="#correlations">Analiza korelacji</a></li>
            <li><a href="#volatility">Analiza zmiennosci</a></li>
            <li><a href="#owners">Porownanie wlascicieli</a></li>
            <li><a href="#summary">Podsumowanie statystyczne</a></li>
        </ul>
    </div>
"""

    def _analysis_section(self, analysis: AnalysisResult) -> str:
        anchor = analysis.name.lower().replace(' ', '-').replace('/', '-')

        insights_html = ""
        if analysis.insights:
            items = "".join([f"<li>{i}</li>" for i in analysis.insights])
            insights_html = f'''
            <div class="insights">
                <h4>Kluczowe wnioski:</h4>
                <ul>{items}</ul>
            </div>'''

        chart_html = ""
        if analysis.chart_interactive:
            chart_html = f'<div class="chart-container">{analysis.chart_interactive}</div>'

        return f"""
    <div class="analysis-section" id="{anchor}">
        <h2>{analysis.name}</h2>
        <p>{analysis.description}</p>
        {chart_html}
        {insights_html}
    </div>
"""

    def _footer(self) -> str:
        return f"""
    <div class="footer">
        <p><strong>Raport wygenerowany:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Zrodlo danych: Bank Danych Lokalnych GUS (bdl.stat.gov.pl)</p>
        <p>System GUS Analytics - Automatyczna analiza danych statystycznych</p>
    </div>
</div>
</body>
</html>
"""