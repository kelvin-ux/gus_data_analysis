from typing import List, Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
import hashlib
import json

from .config import config
from .database import Database, DatabaseQueries
from .validator import DataValidator, ValidationResult, ValidationErrorRepository
from .api_client import GUSApiClient, GUSDataset


@dataclass
class ETLResult:
    success: bool
    import_id: int
    records_processed: int
    records_inserted: int
    records_failed: int
    validation_result: ValidationResult
    duration_seconds: float
    error_message: Optional[str] = None


class ETLPipeline:
    COST_TYPE_MAPPING = {
        "zasoby gminne (komunalne)": ("ZASOBY_GMINNE", "PUBLICZNE"),
        "zasoby Skarbu Państwa": ("ZASOBY_SKARBU_PANSTWA", "PUBLICZNE"),
        "zasoby spółdzielni mieszkaniowych": ("ZASOBY_SPOLDZIELNI", "SPOLDZIELCZE"),
        "zasoby towarzystw budownictwa społecznego (TBS)": ("ZASOBY_TBS", "SPOLECZNE"),
        "zasoby w budynkach objętych wspólnotami mieszkaniowymi": ("ZASOBY_WSPOLNOTY", "PRYWATNE"),
        "zasoby innych podmiotów": ("ZASOBY_INNE", "PRYWATNE"),
        "zasoby zakładów pracy": ("ZASOBY_ZAKLADY_PRACY", "PRYWATNE"),
    }

    def __init__(self, db: Database):
        self.db = db
        self.queries = DatabaseQueries(db)
        self.validator = DataValidator()
        self.error_repo = ValidationErrorRepository(db)
        self.api_client = GUSApiClient()

    def run(
            self,
            years: List[int] = None,
            unit_level: int = 2
    ) -> ETLResult:
        start_time = datetime.now()
        import_id = None

        try:
            import_id = self._start_import("GUS API P3961")

            print("1. Pobieranie danych z API GUS...")
            dataset = self.api_client.fetch_p3961_data(years=years, unit_level=unit_level)
            print(f"   Pobrano {len(dataset.data)} rekordów")

            print("2. Transformacja danych...")
            transformed = self._transform(dataset.data, unit_level)
            print(f"   Przekształcono {len(transformed)} rekordów")

            print("3. Walidacja danych...")
            validation_result = self.validator.validate_raw_batch(transformed)
            print(f"   Poprawnych: {validation_result.valid_count}")
            print(f"   Błędnych: {validation_result.error_count}")

            if validation_result.errors:
                self.error_repo.save_errors(validation_result.errors, import_id)

            print("4. Ładowanie wymiarów...")
            self._load_dimensions(validation_result.valid_records)

            print("5. Ładowanie faktów...")
            inserted = self._load_facts(validation_result.valid_records, import_id)
            print(f"   Załadowano {inserted} rekordów")

            duration = (datetime.now() - start_time).total_seconds()

            self._finish_import(
                import_id=import_id,
                status="SUCCESS",
                rows_processed=len(transformed),
                rows_inserted=inserted,
                rows_failed=validation_result.error_count,
                source_hash=dataset.data_hash
            )

            self._save_quality_report(import_id, validation_result, transformed)

            print(f"\n✓ ETL zakończony w {duration:.2f}s")

            return ETLResult(
                success=True,
                import_id=import_id,
                records_processed=len(transformed),
                records_inserted=inserted,
                records_failed=validation_result.error_count,
                validation_result=validation_result,
                duration_seconds=duration
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()

            if import_id:
                self._finish_import(
                    import_id=import_id,
                    status="FAILED",
                    error_message=str(e)
                )

            print(f"\n✗ ETL błąd: {e}")

            return ETLResult(
                success=False,
                import_id=import_id or 0,
                records_processed=0,
                records_inserted=0,
                records_failed=0,
                validation_result=ValidationResult(),
                duration_seconds=duration,
                error_message=str(e)
            )

    def _transform(self, raw_data: List[Dict], unit_level: int = 2) -> List[Dict]:
        transformed = []

        poziom_map = {
            0: "POLSKA",
            1: "POLSKA",
            2: "WOJEWODZTWO",
            3: "PODREGION",
            4: "POWIAT",
            5: "GMINA",
            6: "GMINA"
        }
        poziom = poziom_map.get(unit_level, "POWIAT")

        for record in raw_data:
            unit_id = record.get("id", "")
            unit_name = record.get("name", "")
            variable_name = record.get("variable_name", "")

            values = record.get("values", [])
            for val in values:
                year = int(val.get("year", 0))
                value = val.get("val")

                if value is None:
                    continue

                kod_gus = self._convert_unit_id(unit_id)
                typ_info = self._map_variable_to_cost_type(variable_name)

                if not typ_info:
                    continue

                typ_kosztu_kod, kategoria = typ_info

                transformed.append({
                    "kod_jednostki": kod_gus,
                    "nazwa_jednostki": unit_name,
                    "poziom": poziom,
                    "kod_wojewodztwa": self._extract_kod_wojewodztwa(kod_gus) if poziom != "POLSKA" else None,
                    "typ_kosztu_kod": typ_kosztu_kod,
                    "typ_kosztu_nazwa": variable_name,
                    "kategoria": kategoria,
                    "rok": year,
                    "wartosc": float(value)
                })

        return transformed

    def _convert_unit_id(self, unit_id: str) -> str:
        clean_id = unit_id.replace("-", "")[:7]
        return clean_id.zfill(7)

    def _determine_poziom(self, kod: str) -> str:
        if kod == "0000000" or kod.startswith("00000"):
            return "POLSKA"
        elif kod.endswith("00000"):
            return "WOJEWODZTWO"
        return "POWIAT"

    def _extract_kod_wojewodztwa(self, kod: str) -> Optional[str]:
        if kod == "0000000":
            return None
        return kod[:2] + "00000"

    def _map_variable_to_cost_type(self, variable_name: str) -> Optional[Tuple[str, str]]:
        # Normalizacja - usuwamy polskie znaki
        def normalize(s):
            replacements = {
                'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n',
                'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
                'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N',
                'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z'
            }
            for pl, ascii in replacements.items():
                s = s.replace(pl, ascii)
            return s.lower()

        name_norm = normalize(variable_name)

        # Bezposrednie dopasowanie slow kluczowych
        if "gminne" in name_norm or "komunalne" in name_norm:
            return ("ZASOBY_GMINNE", "PUBLICZNE")
        if "skarbu panstwa" in name_norm or "skarb panstwa" in name_norm:
            return ("ZASOBY_SKARBU_PANSTWA", "PUBLICZNE")
        if "spoldzielni" in name_norm or "spoldzielcz" in name_norm:
            return ("ZASOBY_SPOLDZIELNI", "SPOLDZIELCZE")
        if "tbs" in name_norm or "budownictwa spolecznego" in name_norm:
            return ("ZASOBY_TBS", "SPOLECZNE")
        if "wspolnot" in name_norm:
            return ("ZASOBY_WSPOLNOTY", "PRYWATNE")
        if "innych podmiotow" in name_norm or "inne podmioty" in name_norm:
            return ("ZASOBY_INNE", "PRYWATNE")
        if "zakladow pracy" in name_norm or "zaklady pracy" in name_norm:
            return ("ZASOBY_ZAKLADY_PRACY", "PRYWATNE")

        # Debug - pokaz niezmapowane
        print(f"  [WARN] Niezmapowana zmienna: {variable_name}")
        return None

    def _load_dimensions(self, records: List[Dict]):
        jednostki = {}
        typy_kosztow = {}
        okresy = set()

        for r in records:
            kod = r["kod_jednostki"]
            if kod not in jednostki:
                jednostki[kod] = {
                    "kod_gus": kod,
                    "nazwa": r["nazwa_jednostki"],
                    "poziom": r["poziom"],
                    "kod_wojewodztwa": r.get("kod_wojewodztwa")
                }

            typ_kod = r["typ_kosztu_kod"]
            if typ_kod not in typy_kosztow:
                typy_kosztow[typ_kod] = {
                    "kod": typ_kod,
                    "nazwa": r["typ_kosztu_nazwa"],
                    "kategoria": r["kategoria"]
                }

            okresy.add(r["rok"])

        for j in jednostki.values():
            self._upsert_jednostka(j)

        for t in typy_kosztow.values():
            self._upsert_typ_kosztu(t)

        for rok in okresy:
            self._upsert_okres(rok)

    def _upsert_jednostka(self, data: Dict):
        existing = self.queries.get_dim_jednostka(data["kod_gus"])
        if existing:
            return existing["id"]

        sql = f"""
            INSERT INTO {self.db.schema}.dim_jednostka (kod_gus, nazwa, poziom, kod_wojewodztwa)
            VALUES (:kod_gus, :nazwa, :poziom, :kod_wojewodztwa)
            ON CONFLICT (kod_gus) DO UPDATE SET nazwa = EXCLUDED.nazwa
            RETURNING id
        """
        result = self.db.fetch_one(sql, data)
        return result["id"] if result else None

    def _upsert_typ_kosztu(self, data: Dict):
        existing = self.queries.get_dim_typ_kosztu(data["kod"])
        if existing:
            return existing["id"]

        sql = f"""
            INSERT INTO {self.db.schema}.dim_typ_kosztu (kod, nazwa, kategoria)
            VALUES (:kod, :nazwa, :kategoria)
            ON CONFLICT (kod) DO UPDATE SET nazwa = EXCLUDED.nazwa
            RETURNING id
        """
        result = self.db.fetch_one(sql, data)
        return result["id"] if result else None

    def _upsert_okres(self, rok: int):
        existing = self.queries.get_dim_okres(rok)
        if existing:
            return existing["id"]

        sql = f"""
            INSERT INTO {self.db.schema}.dim_okres (rok)
            VALUES (:rok)
            ON CONFLICT (rok) DO NOTHING
            RETURNING id
        """
        result = self.db.fetch_one(sql, {"rok": rok})
        return result["id"] if result else None

    def _load_facts(self, records: List[Dict], import_id: int) -> int:
        inserted = 0

        for r in records:
            jednostka_id = self.queries.get_dim_jednostka_id(r["kod_jednostki"])
            typ_kosztu_id = self.queries.get_dim_typ_kosztu_id(r["typ_kosztu_kod"])
            okres_id = self.queries.get_dim_okres_id(r["rok"])

            if not all([jednostka_id, typ_kosztu_id, okres_id]):
                continue

            sql = f"""
                INSERT INTO {self.db.schema}.fact_koszty 
                (jednostka_id, typ_kosztu_id, okres_id, wartosc, import_id)
                VALUES (:jednostka_id, :typ_kosztu_id, :okres_id, :wartosc, :import_id)
                ON CONFLICT (jednostka_id, typ_kosztu_id, okres_id) 
                DO UPDATE SET wartosc = EXCLUDED.wartosc, updated_at = CURRENT_TIMESTAMP
            """

            self.db.execute(sql, {
                "jednostka_id": jednostka_id,
                "typ_kosztu_id": typ_kosztu_id,
                "okres_id": okres_id,
                "wartosc": r["wartosc"],
                "import_id": import_id
            })
            inserted += 1

        return inserted

    def _start_import(self, source: str) -> int:
        sql = f"""
            INSERT INTO {self.db.schema}.log_import (source_file, status)
            VALUES (:source, 'RUNNING')
            RETURNING id
        """
        result = self.db.fetch_one(sql, {"source": source})
        return result["id"]

    def _finish_import(
            self,
            import_id: int,
            status: str,
            rows_processed: int = 0,
            rows_inserted: int = 0,
            rows_failed: int = 0,
            source_hash: str = None,
            error_message: str = None
    ):
        sql = f"""
            UPDATE {self.db.schema}.log_import
            SET finished_at = CURRENT_TIMESTAMP,
                status = :status,
                rows_processed = :rows_processed,
                rows_inserted = :rows_inserted,
                rows_failed = :rows_failed,
                source_hash = :source_hash,
                error_message = :error_message
            WHERE id = :import_id
        """
        self.db.execute(sql, {
            "import_id": import_id,
            "status": status,
            "rows_processed": rows_processed,
            "rows_inserted": rows_inserted,
            "rows_failed": rows_failed,
            "source_hash": source_hash,
            "error_message": error_message
        })

    def _save_quality_report(
            self,
            import_id: int,
            validation: ValidationResult,
            all_records: List[Dict]
    ):
        values = [r["wartosc"] for r in all_records if r.get("wartosc") is not None]

        if values:
            import statistics
            min_val = min(values)
            max_val = max(values)
            avg_val = statistics.mean(values)
            median_val = statistics.median(values)
            stddev_val = statistics.stdev(values) if len(values) > 1 else 0
        else:
            min_val = max_val = avg_val = median_val = stddev_val = None

        issues = [{"type": e.error_type.value, "field": e.error_field, "message": e.error_message}
                  for e in validation.errors[:100]]

        sql = f"""
            INSERT INTO {self.db.schema}.data_quality_report
            (import_id, total_rows, null_count, null_percentage, validation_passed,
             issues, min_value, max_value, avg_value, median_value, stddev_value)
            VALUES
            (:import_id, :total_rows, :null_count, :null_percentage, :validation_passed,
             :issues, :min_value, :max_value, :avg_value, :median_value, :stddev_value)
        """

        self.db.execute(sql, {
            "import_id": import_id,
            "total_rows": validation.total_input,
            "null_count": validation.error_count,
            "null_percentage": round((1 - validation.success_rate / 100) * 100, 2),
            "validation_passed": validation.error_count == 0,
            "issues": json.dumps(issues),
            "min_value": min_val,
            "max_value": max_val,
            "avg_value": avg_val,
            "median_value": median_val,
            "stddev_value": stddev_val
        })