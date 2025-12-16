import requests
import hashlib
import json
import time
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

from .config import config


@dataclass
class GUSDataset:
    subject_id: str
    name: str
    data: List[Dict]
    metadata: Dict
    fetched_at: datetime
    data_hash: str


class GUSApiClient:

    def __init__(
            self,
            base_url: str = None,
            api_key: str = None,
            timeout: int = None,
            retry_count: int = None,
            retry_delay: float = None
    ):
        self.base_url = base_url or config.api.base_url
        self.api_key = api_key or config.api.api_key
        self.timeout = timeout or config.api.timeout
        self.retry_count = retry_count or config.api.retry_count
        self.retry_delay = retry_delay or config.api.retry_delay

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "GUS-Analytics-Client/1.0"
        })
        if self.api_key:
            self.session.headers["X-ClientId"] = self.api_key

    def _request(
            self,
            endpoint: str,
            params: Optional[Dict] = None
    ) -> Optional[Dict]:
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(self.retry_count):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)

                if response.status_code == 429:
                    wait_time = self.retry_delay * (2 ** attempt)
                    time.sleep(wait_time)
                    continue

                if response.status_code == 404:
                    return None

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

        return None

    def _paginate(
            self,
            endpoint: str,
            params: Optional[Dict] = None,
            page_size: int = None
    ) -> List[Dict]:
        all_results = []
        page = 0
        page_size = page_size or config.api.page_size

        while True:
            request_params = params.copy() if params else {}
            request_params["page-size"] = page_size
            request_params["page"] = page

            response = self._request(endpoint, request_params)

            if not response or "results" not in response:
                break

            all_results.extend(response["results"])

            total = response.get("totalRecords", 0)
            if len(all_results) >= total:
                break

            page += 1
            time.sleep(0.1)

        return all_results

    def get_subjects(self, parent_id: Optional[str] = None) -> List[Dict]:
        params = {"parent-id": parent_id} if parent_id else {}
        response = self._request("subjects", params)
        return response.get("results", []) if response else []

    def get_subject(self, subject_id: str) -> Optional[Dict]:
        return self._request(f"subjects/{subject_id}")

    def get_variables(self, subject_id: str) -> List[Dict]:
        return self._paginate("variables", {"subject-id": subject_id})

    def get_variable(self, variable_id: str) -> Optional[Dict]:
        return self._request(f"variables/{variable_id}")

    def get_units(self, level: int = 5) -> List[Dict]:
        return self._paginate("units", {"level": level}, page_size=500)

    def get_unit(self, unit_id: str) -> Optional[Dict]:
        return self._request(f"units/{unit_id}")

    def get_data_by_variable(
            self,
            variable_id: str,
            years: List[int] = None,
            unit_level: int = 2
    ) -> List[Dict]:
        if years is None:
            years = [2018, 2020, 2022, 2024]

        all_data = []

        for year in years:
            params = {
                "year": year,
                "unit-level": unit_level
            }

            endpoint = f"data/by-variable/{variable_id}"

            page = 0
            page_size = 100

            while True:
                request_params = params.copy()
                request_params["page-size"] = page_size
                request_params["page"] = page

                response = self._request(endpoint, request_params)

                if not response or "results" not in response:
                    break

                for record in response["results"]:
                    record["year"] = year
                all_data.extend(response["results"])

                total = response.get("totalRecords", 0)
                fetched = (page + 1) * page_size
                if fetched >= total or len(response["results"]) < page_size:
                    break

                page += 1
                time.sleep(0.1)

            time.sleep(0.1)

        return all_data

    def get_data_by_unit(
            self,
            unit_id: str,
            variable_id: str,
            year_from: int = 2018,
            year_to: int = 2024
    ) -> List[Dict]:
        params = {
            "var-id": variable_id,
            "year-from": year_from,
            "year-to": year_to
        }
        return self._paginate(f"data/by-unit/{unit_id}", params)

    def check_subject_update(self, subject_id: str) -> Optional[str]:
        subject = self.get_subject(subject_id)
        if subject:
            return subject.get("lastUpdate")
        return None

    def fetch_p3961_data(
            self,
            years: List[int] = None,
            unit_level: int = 2
    ) -> GUSDataset:
        if years is None:
            years = [2018, 2020, 2022, 2024]

        variables = self.get_variables(config.api.subgroup_id)

        all_data = []

        for var in variables:
            var_id = var.get("id")
            var_name = var.get("n1", "")

            data = self.get_data_by_variable(
                variable_id=str(var_id),
                years=years,
                unit_level=unit_level
            )

            for record in data:
                record["variable_id"] = var_id
                record["variable_name"] = var_name

            all_data.extend(data)

        data_hash = self._calculate_hash(all_data)

        return GUSDataset(
            subject_id=config.api.subgroup_id,
            name="Koszty utrzymania mieszkań i lokali użytkowych",
            data=all_data,
            metadata={
                "years": years,
                "unit_level": unit_level,
                "variables_count": len(variables),
                "records_count": len(all_data)
            },
            fetched_at=datetime.now(),
            data_hash=data_hash
        )

    def _calculate_hash(self, data: List[Dict]) -> str:
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()


class GUSDataCache:

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or config.paths.data_dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def save(self, dataset: GUSDataset) -> Path:
        timestamp = dataset.fetched_at.strftime("%Y%m%d_%H%M%S")
        filename = f"{dataset.subject_id}_{timestamp}.json"
        filepath = self.cache_dir / filename

        cache_data = {
            "subject_id": dataset.subject_id,
            "name": dataset.name,
            "data": dataset.data,
            "metadata": dataset.metadata,
            "fetched_at": dataset.fetched_at.isoformat(),
            "data_hash": dataset.data_hash
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        return filepath

    def load_latest(self, subject_id: str) -> Optional[GUSDataset]:
        pattern = f"{subject_id}_*.json"
        files = sorted(self.cache_dir.glob(pattern), reverse=True)

        if not files:
            return None

        with open(files[0], "r", encoding="utf-8") as f:
            cache_data = json.load(f)

        return GUSDataset(
            subject_id=cache_data["subject_id"],
            name=cache_data["name"],
            data=cache_data["data"],
            metadata=cache_data["metadata"],
            fetched_at=datetime.fromisoformat(cache_data["fetched_at"]),
            data_hash=cache_data["data_hash"]
        )

    def get_latest_hash(self, subject_id: str) -> Optional[str]:
        dataset = self.load_latest(subject_id)
        return dataset.data_hash if dataset else None

    def has_changed(self, new_hash: str, subject_id: str) -> bool:
        old_hash = self.get_latest_hash(subject_id)
        return old_hash != new_hash


class CSVDataLoader:
    COST_TYPE_MAPPING = {
        "ogółem (łącznie z kosztami c.o. i c.w)": ("OGOLEM_Z_CO_CW", "OGOLEM"),
        "ogółem (bez kosztów c.o i c.w)": ("OGOLEM_BEZ_CO_CW", "OGOLEM"),
        "koszty eksploatacji razem": ("EKSPLOATACJA_RAZEM", "EKSPLOATACJA"),
        "koszty eksploatacji - zarządu i administracyjno-biurowe": ("EKSPLOATACJA_ZARZAD", "EKSPLOATACJA"),
        "koszty eksploatacji - konserwacji i remontów": ("EKSPLOATACJA_KONSERWACJA", "EKSPLOATACJA"),
        "koszty świadczonych usług razem (łącznie z kosztami c.o. i c.w)": ("USLUGI_RAZEM", "USLUGI"),
        "koszty świadczonych usług - centralne ogrzewanie i ciepła woda": ("USLUGI_CO_CW", "USLUGI"),
        "koszty świadczonych usług - zimna woda, odprowadzanie ścieków lub odbiór nieczystości ciekłych": (
        "USLUGI_WODA", "USLUGI"),
        "koszty świadczonych usług - odbieranie odpadów komunalnych": ("USLUGI_ODPADY", "USLUGI"),
        "koszty świadczonych usług - utrzymanie wind": ("USLUGI_WINDY", "USLUGI"),
    }

    @classmethod
    def load(cls, filepath: Path) -> List[Dict]:
        import csv

        records = []

        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            headers = next(reader)

            for row in reader:
                if len(row) < 2:
                    continue

                kod = str(row[0]).zfill(7)
                nazwa = row[1]

                for i, header in enumerate(headers[2:], start=2):
                    parts = header.split(";")
                    if len(parts) < 4:
                        continue

                    typ_kosztu_nazwa = parts[1].strip()
                    rok = parts[2].strip()

                    value = row[i] if i < len(row) else None

                    if value is None or value.strip() == "":
                        continue

                    try:
                        wartosc = float(value.replace(",", "."))
                    except ValueError:
                        continue

                    typ_info = cls.COST_TYPE_MAPPING.get(typ_kosztu_nazwa)
                    if not typ_info:
                        continue

                    typ_kosztu_kod, kategoria = typ_info

                    records.append({
                        "kod_jednostki": kod,
                        "nazwa_jednostki": nazwa,
                        "typ_kosztu_kod": typ_kosztu_kod,
                        "typ_kosztu_nazwa": typ_kosztu_nazwa,
                        "kategoria": kategoria,
                        "rok": int(rok),
                        "wartosc": wartosc
                    })

        return records

    @classmethod
    def determine_poziom(cls, kod: str) -> str:
        if kod == "0000000":
            return "POLSKA"
        elif kod.endswith("00000"):
            return "WOJEWODZTWO"
        return "POWIAT"

    @classmethod
    def extract_kod_wojewodztwa(cls, kod: str) -> Optional[str]:
        if kod == "0000000":
            return None
        return kod[:2] + "00000"