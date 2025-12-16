from src.database import Database
from src.config import config
from src.etl import ETLPipeline

db = Database(
    host=config.db.host,
    port=config.db.port,
    database=config.db.database,
    user=config.db.user,
    password=config.db.password,
    schema=config.db.schema
)

print("=== ETL Pipeline ===\n")

pipeline = ETLPipeline(db)

result = pipeline.run(
    years=[2018, 2020, 2022, 2024],
    unit_level=2
)

print("\n=== Podsumowanie ===")
print(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
print(f"Import ID: {result.import_id}")
print(f"Przetworzono: {result.records_processed}")
print(f"Załadowano: {result.records_inserted}")
print(f"Błędów: {result.records_failed}")
print(f"Czas: {result.duration_seconds:.2f}s")

if result.error_message:
    print(f"Błąd: {result.error_message}")