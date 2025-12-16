from src.database import Database
from src.config import config

db = Database(
    host=config.db.host,
    port=config.db.port,
    database=config.db.database,
    user=config.db.user,
    password=config.db.password,
    schema=config.db.schema
)

result = db.fetch_one("SELECT version()")
print(f"PostgreSQL: {result}")

tables = db.fetch_all("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'gus'
    ORDER BY table_name
""")

print(f"\nTabele w schemacie 'gus':")
for t in tables:
    print(f"  - {t['table_name']}")
