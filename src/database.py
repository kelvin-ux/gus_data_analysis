from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager


class Database:
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "gus_analytics",
        user: str = "postgres",
        password: str | None = None,
        schema: str = "gus"
    ):
        self.schema = schema

        if password:
            self.connection_string = (
                f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
            )
        else:
            self.connection_string = (
                f"postgresql+psycopg://{user}@{host}:{port}/{database}"
            )

        self.engine = create_engine(
            self.connection_string,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )

        self.Session = sessionmaker(bind=self.engine)
    
    @contextmanager
    def session(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @contextmanager
    def connection(self):
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    def execute(self, sql: str, params: Optional[Dict] = None) -> Any:
        with self.connection() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result
    
    def fetch_all(self, sql: str, params: Optional[Dict] = None) -> List[Dict]:
        with self.connection() as conn:
            result = conn.execute(text(sql), params or {})
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
    
    def fetch_one(self, sql: str, params: Optional[Dict] = None) -> Optional[Dict]:
        with self.connection() as conn:
            result = conn.execute(text(sql), params or {})
            row = result.fetchone()
            conn.commit()
            if row:
                return dict(zip(result.keys(), row))
            return None
    
    def init_schema(self, sql_file: Path):
        with open(sql_file, 'r') as f:
            sql = f.read()
        
        with self.connection() as conn:
            conn.execute(text(sql))
            conn.commit()
    
    def set_search_path(self):
        self.execute(f"SET search_path TO {self.schema}")
    
    def table_exists(self, table_name: str) -> bool:
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :table
            )
        """
        result = self.fetch_one(sql, {"schema": self.schema, "table": table_name})
        return result.get("exists", False) if result else False
    
    def get_row_count(self, table_name: str) -> int:
        sql = f"SELECT COUNT(*) as cnt FROM {self.schema}.{table_name}"
        result = self.fetch_one(sql)
        return result.get("cnt", 0) if result else 0
    
    def insert_many(self, table_name: str, records: List[Dict]) -> int:
        if not records:
            return 0
        
        columns = records[0].keys()
        col_names = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        
        sql = f"INSERT INTO {self.schema}.{table_name} ({col_names}) VALUES ({placeholders})"
        
        with self.session() as session:
            for record in records:
                session.execute(text(sql), record)
        
        return len(records)
    
    def upsert(self, table_name: str, records: List[Dict], conflict_columns: List[str]) -> int:
        if not records:
            return 0
        
        columns = records[0].keys()
        col_names = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        conflict_cols = ", ".join(conflict_columns)
        update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
        
        sql = f"""
            INSERT INTO {self.schema}.{table_name} ({col_names}) 
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_cols}
        """
        
        with self.session() as session:
            for record in records:
                session.execute(text(sql), record)
        
        return len(records)


class DatabaseQueries:
    
    def __init__(self, db: Database):
        self.db = db
        self.schema = db.schema
    
    def get_dim_jednostka(self, kod_gus: str) -> Optional[Dict]:
        sql = f"SELECT * FROM {self.schema}.dim_jednostka WHERE kod_gus = :kod"
        return self.db.fetch_one(sql, {"kod": kod_gus})
    
    def get_dim_jednostka_id(self, kod_gus: str) -> Optional[int]:
        result = self.get_dim_jednostka(kod_gus)
        return result.get("id") if result else None
    
    def get_dim_typ_kosztu(self, kod: str) -> Optional[Dict]:
        sql = f"SELECT * FROM {self.schema}.dim_typ_kosztu WHERE kod = :kod"
        return self.db.fetch_one(sql, {"kod": kod})
    
    def get_dim_typ_kosztu_id(self, kod: str) -> Optional[int]:
        result = self.get_dim_typ_kosztu(kod)
        return result.get("id") if result else None
    
    def get_dim_okres(self, rok: int) -> Optional[Dict]:
        sql = f"SELECT * FROM {self.schema}.dim_okres WHERE rok = :rok"
        return self.db.fetch_one(sql, {"rok": rok})
    
    def get_dim_okres_id(self, rok: int) -> Optional[int]:
        result = self.get_dim_okres(rok)
        return result.get("id") if result else None
    
    def get_all_jednostki(self, poziom: Optional[str] = None) -> List[Dict]:
        if poziom:
            sql = f"SELECT * FROM {self.schema}.dim_jednostka WHERE poziom = :poziom ORDER BY nazwa"
            return self.db.fetch_all(sql, {"poziom": poziom})
        sql = f"SELECT * FROM {self.schema}.dim_jednostka ORDER BY poziom, nazwa"
        return self.db.fetch_all(sql)
    
    def get_all_typy_kosztow(self, kategoria: Optional[str] = None) -> List[Dict]:
        if kategoria:
            sql = f"SELECT * FROM {self.schema}.dim_typ_kosztu WHERE kategoria = :kategoria ORDER BY nazwa"
            return self.db.fetch_all(sql, {"kategoria": kategoria})
        sql = f"SELECT * FROM {self.schema}.dim_typ_kosztu ORDER BY kategoria, nazwa"
        return self.db.fetch_all(sql)
    
    def get_all_okresy(self) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.dim_okres ORDER BY rok"
        return self.db.fetch_all(sql)
    
    def get_koszty_pelne(self, rok: Optional[int] = None, poziom: Optional[str] = None) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.v_koszty_pelne WHERE 1=1"
        params = {}
        
        if rok:
            sql += " AND rok = :rok"
            params["rok"] = rok
        if poziom:
            sql += " AND poziom = :poziom"
            params["poziom"] = poziom
        
        sql += " ORDER BY rok, jednostka_nazwa"
        return self.db.fetch_all(sql, params)
    
    def get_koszty_wojewodztwa(self, rok: Optional[int] = None) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.v_koszty_wojewodztwa"
        params = {}
        
        if rok:
            sql += " WHERE rok = :rok"
            params["rok"] = rok
        
        sql += " ORDER BY rok, suma_kosztow DESC"
        return self.db.fetch_all(sql, params)
    
    def get_trend_roczny(self) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.v_trend_roczny ORDER BY poziom, rok"
        return self.db.fetch_all(sql)
    
    def get_struktura_kosztow(self, rok: Optional[int] = None) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.v_struktura_kosztow"
        params = {}
        
        if rok:
            sql += " WHERE rok = :rok"
            params["rok"] = rok
        
        return self.db.fetch_all(sql, params)
    
    def get_top_zmiany(self, limit: int = 10) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.v_top_zmiany LIMIT :limit"
        return self.db.fetch_all(sql, {"limit": limit})
    
    def get_ostatni_import(self) -> Optional[Dict]:
        sql = f"SELECT * FROM {self.schema}.v_ostatni_import"
        return self.db.fetch_one(sql)
    
    def get_audit_log(self, table_name: Optional[str] = None, limit: int = 100) -> List[Dict]:
        sql = f"SELECT * FROM {self.schema}.audit_log"
        params = {"limit": limit}
        
        if table_name:
            sql += " WHERE table_name = :table_name"
            params["table_name"] = table_name
        
        sql += " ORDER BY changed_at DESC LIMIT :limit"
        return self.db.fetch_all(sql, params)