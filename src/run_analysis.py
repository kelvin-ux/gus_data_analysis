from src.database import Database
from src.config import config
from src.analysis import DataAnalyzer
from src.report import ReportGenerator, HTMLReportGenerator

db = Database(
    host=config.db.host,
    port=config.db.port,
    database=config.db.database,
    user=config.db.user,
    password=config.db.password,
    schema=config.db.schema
)

print("=== Analiza danych GUS ===\n")

analyzer = DataAnalyzer(db)

print("Pobieranie statystyk...")
stats = analyzer.get_summary_stats()
print(f"  Rekordów: {stats['total_records']}")
print(f"  Lata: {stats['years']}")
print(f"  Województw: {stats['regions_count']}")
print(f"  Suma kosztów: {stats['total_value']/1000:.1f} mln zł")

print("\n--- Uruchamianie analiz ---\n")
analyses = analyzer.run_all_analyses()

print("\n--- Generowanie raportów ---\n")

print("Generowanie PDF...")
pdf_gen = ReportGenerator()
pdf_path = pdf_gen.generate(analyses, stats)
print(f"  PDF: {pdf_path}")

print("Generowanie HTML...")
html_gen = HTMLReportGenerator()
html_path = html_gen.generate(analyses, stats)
print(f"  HTML: {html_path}")