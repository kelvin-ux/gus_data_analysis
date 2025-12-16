from src.database import Database
from src.config import config
from pathlib import Path

db = Database(
    host=config.db.host,
    port=config.db.port,
    database=config.db.database,
    user=config.db.user,
    password=config.db.password,
    schema=config.db.schema
)

db.init_schema(Path("db/models.sql"))
print("db done")
