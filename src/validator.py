import re
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class ErrorType(Enum):
    NULL_VALUE = "NULL_VALUE"
    INVALID_KOD_GUS = "INVALID_KOD_GUS"
    INVALID_DATA_TYPE = "INVALID_DATA_TYPE"
    INVALID_POZIOM = "INVALID_POZIOM"
    INVALID_KATEGORIA = "INVALID_KATEGORIA"
    INVALID_ROK = "INVALID_ROK"


@dataclass
class ValidationError:
    record_data: Dict
    error_type: ErrorType
    error_field: str
    error_message: str
    raw_value: Any = None


@dataclass
class ValidationResult:
    valid_records: List[Dict] = field(default_factory=list)
    errors: List[ValidationError] = field(default_factory=list)

    @property
    def total_input(self) -> int:
        return len(self.valid_records) + len(self.errors)

    @property
    def valid_count(self) -> int:
        return len(self.valid_records)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def success_rate(self) -> float:
        if self.total_input == 0:
            return 0.0
        return self.valid_count / self.total_input * 100


class DataValidator:
    KOD_GUS_PATTERN = re.compile(r'^\d{7}$')
    VALID_POZIOMY = {'POLSKA', 'WOJEWODZTWO', 'POWIAT'}
    VALID_KATEGORIE = {'PUBLICZNE', 'SPOLDZIELCZE', 'SPOLECZNE', 'PRYWATNE'}
    MIN_ROK = 2000
    MAX_ROK = 2100

    REQUIRED_FIELDS_JEDNOSTKA = ['kod_gus', 'nazwa', 'poziom']
    REQUIRED_FIELDS_TYP_KOSZTU = ['kod', 'nazwa', 'kategoria']
    REQUIRED_FIELDS_OKRES = ['rok']
    REQUIRED_FIELDS_FAKT = ['jednostka_id', 'typ_kosztu_id', 'okres_id', 'wartosc']

    def __init__(self):
        self.errors: List[ValidationError] = []

    def _add_error(
            self,
            record: Dict,
            error_type: ErrorType,
            field: str,
            message: str,
            raw_value: Any = None
    ):
        self.errors.append(ValidationError(
            record_data=record.copy(),
            error_type=error_type,
            error_field=field,
            error_message=message,
            raw_value=str(raw_value) if raw_value is not None else None
        ))

    def _check_nulls(self, record: Dict, required_fields: List[str]) -> bool:
        for field in required_fields:
            value = record.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                self._add_error(
                    record,
                    ErrorType.NULL_VALUE,
                    field,
                    f"Pole '{field}' jest puste lub null",
                    value
                )
                return False
        return True

    def _check_kod_gus(self, record: Dict) -> bool:
        kod = record.get('kod_gus', '')
        if not self.KOD_GUS_PATTERN.match(str(kod)):
            self._add_error(
                record,
                ErrorType.INVALID_KOD_GUS,
                'kod_gus',
                f"Nieprawidłowy format kodu GUS (wymagane 7 cyfr)",
                kod
            )
            return False
        return True

    def _check_poziom(self, record: Dict) -> bool:
        poziom = record.get('poziom', '')
        if poziom not in self.VALID_POZIOMY:
            self._add_error(
                record,
                ErrorType.INVALID_POZIOM,
                'poziom',
                f"Nieprawidłowy poziom (dozwolone: {self.VALID_POZIOMY})",
                poziom
            )
            return False
        return True

    def _check_kategoria(self, record: Dict) -> bool:
        kategoria = record.get('kategoria', '')
        if kategoria not in self.VALID_KATEGORIE:
            self._add_error(
                record,
                ErrorType.INVALID_KATEGORIA,
                'kategoria',
                f"Nieprawidłowa kategoria (dozwolone: {self.VALID_KATEGORIE})",
                kategoria
            )
            return False
        return True

    def _check_rok(self, record: Dict) -> bool:
        rok = record.get('rok')
        try:
            rok_int = int(rok)
            if not (self.MIN_ROK <= rok_int <= self.MAX_ROK):
                self._add_error(
                    record,
                    ErrorType.INVALID_ROK,
                    'rok',
                    f"Rok poza zakresem ({self.MIN_ROK}-{self.MAX_ROK})",
                    rok
                )
                return False
        except (TypeError, ValueError):
            self._add_error(
                record,
                ErrorType.INVALID_DATA_TYPE,
                'rok',
                "Rok musi być liczbą całkowitą",
                rok
            )
            return False
        return True

    def _check_numeric(self, record: Dict, field: str) -> bool:
        value = record.get(field)
        if value is None:
            return True
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            self._add_error(
                record,
                ErrorType.INVALID_DATA_TYPE,
                field,
                f"Pole '{field}' musi być liczbą",
                value
            )
            return False

    def validate_jednostka(self, record: Dict) -> bool:
        if not self._check_nulls(record, self.REQUIRED_FIELDS_JEDNOSTKA):
            return False
        if not self._check_kod_gus(record):
            return False
        if not self._check_poziom(record):
            return False
        return True

    def validate_typ_kosztu(self, record: Dict) -> bool:
        if not self._check_nulls(record, self.REQUIRED_FIELDS_TYP_KOSZTU):
            return False
        if not self._check_kategoria(record):
            return False
        return True

    def validate_okres(self, record: Dict) -> bool:
        if not self._check_nulls(record, self.REQUIRED_FIELDS_OKRES):
            return False
        if not self._check_rok(record):
            return False
        return True

    def validate_fakt(self, record: Dict) -> bool:
        if not self._check_nulls(record, self.REQUIRED_FIELDS_FAKT):
            return False
        if not self._check_numeric(record, 'wartosc'):
            return False
        return True

    def validate_batch(
            self,
            records: List[Dict],
            record_type: str
    ) -> ValidationResult:
        self.errors = []
        valid_records = []

        validator_map = {
            'jednostka': self.validate_jednostka,
            'typ_kosztu': self.validate_typ_kosztu,
            'okres': self.validate_okres,
            'fakt': self.validate_fakt
        }

        validator = validator_map.get(record_type)
        if not validator:
            raise ValueError(f"Nieznany typ rekordu: {record_type}")

        for record in records:
            if validator(record):
                valid_records.append(record)

        return ValidationResult(
            valid_records=valid_records,
            errors=self.errors.copy()
        )

    def validate_raw_gus_record(self, record: Dict) -> bool:
        kod = record.get('kod_jednostki', '')
        if not self.KOD_GUS_PATTERN.match(str(kod).zfill(7)):
            self._add_error(
                record,
                ErrorType.INVALID_KOD_GUS,
                'kod_jednostki',
                "Nieprawidłowy format kodu GUS",
                kod
            )
            return False

        nazwa = record.get('nazwa_jednostki')
        if not nazwa or str(nazwa).strip() == '':
            self._add_error(
                record,
                ErrorType.NULL_VALUE,
                'nazwa_jednostki',
                "Brak nazwy jednostki",
                nazwa
            )
            return False

        rok = record.get('rok')
        try:
            rok_int = int(rok)
            if not (self.MIN_ROK <= rok_int <= self.MAX_ROK):
                self._add_error(
                    record,
                    ErrorType.INVALID_ROK,
                    'rok',
                    f"Rok poza zakresem",
                    rok
                )
                return False
        except (TypeError, ValueError):
            self._add_error(
                record,
                ErrorType.INVALID_DATA_TYPE,
                'rok',
                "Nieprawidłowy rok",
                rok
            )
            return False

        wartosc = record.get('wartosc')
        if wartosc is None:
            self._add_error(
                record,
                ErrorType.NULL_VALUE,
                'wartosc',
                "Brak wartości",
                wartosc
            )
            return False

        return True

    def validate_raw_batch(self, records: List[Dict]) -> ValidationResult:
        self.errors = []
        valid_records = []

        for record in records:
            if self.validate_raw_gus_record(record):
                valid_records.append(record)

        return ValidationResult(
            valid_records=valid_records,
            errors=self.errors.copy()
        )


class ValidationErrorRepository:

    def __init__(self, db):
        self.db = db
        self.schema = db.schema

    def save_errors(self, errors: List[ValidationError], import_id: Optional[int] = None) -> int:
        if not errors:
            return 0

        import json

        records = []
        for error in errors:
            records.append({
                'import_id': import_id,
                'record_data': json.dumps(error.record_data),
                'error_type': error.error_type.value,
                'error_field': error.error_field,
                'error_message': error.error_message,
                'raw_value': error.raw_value
            })

        sql = f"""
            INSERT INTO {self.schema}.validation_errors 
            (import_id, record_data, error_type, error_field, error_message, raw_value)
            VALUES (:import_id, :record_data::jsonb, :error_type, :error_field, :error_message, :raw_value)
        """

        with self.db.session() as session:
            from sqlalchemy import text
            for record in records:
                session.execute(text(sql), record)

        return len(records)

    def get_errors_by_import(self, import_id: int) -> List[Dict]:
        sql = f"""
            SELECT * FROM {self.schema}.validation_errors 
            WHERE import_id = :import_id 
            ORDER BY created_at
        """
        return self.db.fetch_all(sql, {'import_id': import_id})

    def get_error_summary(self, import_id: int) -> Dict:
        sql = f"""
            SELECT 
                error_type,
                error_field,
                COUNT(*) as count
            FROM {self.schema}.validation_errors
            WHERE import_id = :import_id
            GROUP BY error_type, error_field
            ORDER BY count DESC
        """
        rows = self.db.fetch_all(sql, {'import_id': import_id})

        return {
            'by_type': rows,
            'total': sum(r['count'] for r in rows)
        }