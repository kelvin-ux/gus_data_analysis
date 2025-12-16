from src.database import Database
from src.config import config
from src.etl import ETLPipeline
from pathlib import Path

db = Database(
    host=config.db.host,
    port=config.db.port,
    database=config.db.database,
    user=config.db.user,
    password=config.db.password,
    schema=config.db.schema
)

print("=== Reset i pełny pipeline ===\n")

print("1. Reset schematu...")
try:
    db.execute(f"DROP SCHEMA IF EXISTS {db.schema} CASCADE")
    print("   Schema usunięty")
except Exception as e:
    print(f"   Błąd: {e}")

print("2. Tworzenie schematu...")
db.init_schema(Path("db/models.sql"))
print("   Schema utworzony")

print("\n3. Uruchamianie ETL...")
pipeline = ETLPipeline(db)
result = pipeline.run(years=[2022, 2024], unit_level=2)

print(f"\n=== Wynik ETL ===")
print(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
print(f"Załadowano: {result.records_inserted} rekordów")

if result.success:
    print("\n4. Weryfikacja danych...")

    poziomy = db.fetch_all(f"SELECT poziom, COUNT(*) as cnt FROM {db.schema}.dim_jednostka GROUP BY poziom")
    print(f"   Poziomy: {poziomy}")

    total = db.fetch_one(f"SELECT COUNT(*) as cnt FROM {db.schema}.fact_koszty")
    print(f"   Faktów: {total}")

print("\n✓ Gotowe! Teraz uruchom: python run_analysis.py")