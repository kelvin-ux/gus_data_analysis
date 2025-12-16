import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "gus_analytics")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "")
    schema: str = os.getenv("DB_SCHEMA", "gus")
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class GUSApiConfig:
    base_url: str = "https://bdl.stat.gov.pl/api/v1"
    api_key: str = os.getenv("GUS_API_KEY", "")
    timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 1.0
    page_size: int = 100
    
    subject_id: str = "K11"
    group_id: str = "G621"
    subgroup_id: str = "P3961"


@dataclass
class PathsConfig:
    base_dir: Path = Path(__file__).parent.parent
    sql_dir: Path = None
    data_dir: Path = None
    logs_dir: Path = None
    output_dir: Path = None
    
    def __post_init__(self):
        self.sql_dir = self.base_dir / "sql"
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.output_dir = self.base_dir / "output"
        
        for dir_path in [self.data_dir, self.logs_dir, self.output_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @property
    def schema_file(self) -> Path:
        return self.sql_dir / "01_schema.sql"


@dataclass
class ValidationConfig:
    min_rok: int = 2000
    max_rok: int = 2100
    kod_gus_length: int = 7
    strict_mode: bool = True


@dataclass
class Config:
    db: DatabaseConfig = None
    api: GUSApiConfig = None
    paths: PathsConfig = None
    validation: ValidationConfig = None
    
    def __post_init__(self):
        self.db = DatabaseConfig()
        self.api = GUSApiConfig()
        self.paths = PathsConfig()
        self.validation = ValidationConfig()


config = Config()