import schedule
import time
from datetime import datetime
from typing import Callable
import argparse
from pathlib import Path

from .database import Database
from .config import config
from .api_client import GUSApiClient, GUSDataCache
from .etl import ETLPipeline
from .analysis import DataAnalyzer
from .report import ReportGenerator, HTMLReportGenerator
from .alerts import EmailAlert

class Scheduler:

    def __init__(self):
        self.db = Database(
            host=config.db.host,
            port=config.db.port,
            database=config.db.database,
            user=config.db.user,
            password=config.db.password,
            schema=config.db.schema
        )
        self.api_client = GUSApiClient()
        self.cache = GUSDataCache()
        self.email = EmailAlert()

    def check_for_updates(self) -> bool:
        print(f"[{datetime.now()}] Sprawdzanie aktualizacji GUS...")

        try:
            dataset = self.api_client.fetch_p3961_data(years=[2024], unit_level=2)
            new_hash = dataset.data_hash

            if self.cache.has_changed(new_hash, config.api.subgroup_id):
                print("Wykryto nowe dane!")
                self.cache.save(dataset)
                return True
            else:
                print("Brak nowych danych")
                return False

        except Exception as e:
            print(f"Blad sprawdzania: {e}")
            return False

    def run_etl(self, years=None) -> bool:
        print(f"[{datetime.now()}] Uruchamianie ETL...")

        if years is None:
            years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]

        try:
            pipeline = ETLPipeline(self.db)
            result = pipeline.run(years=years, unit_level=2)

            if result.success:
                print(f"ETL sukces: {result.records_inserted} rekordow")
                return True
            else:
                print(f"ETL blad: {result.error_message}")
                self.email.send_etl_failure(result.error_message)
                return False

        except Exception as e:
            print(f"ETL exception: {e}")
            self.email.send_etl_failure(str(e))
            return False

    def run_analysis(self) -> Path:
        print(f"[{datetime.now()}] Uruchamianie analizy...")

        analyzer = DataAnalyzer(self.db)
        analyses = analyzer.run_all_analyses()
        stats = analyzer.get_summary_stats()

        pdf_generator = ReportGenerator()
        pdf_path = pdf_generator.generate(analyses, stats)
        print(f"Raport PDF: {pdf_path}")

        html_generator = HTMLReportGenerator()
        html_path = html_generator.generate(analyses, stats)
        print(f"Raport HTML: {html_path}")

        return pdf_path

    def weekly_job(self):
        print(f"\n{'=' * 50}")
        print(f"[{datetime.now()}] TYGODNIOWE ZADANIE START")
        print(f"{'=' * 50}\n")

        has_updates = self.check_for_updates()

        if has_updates:
            self.email.send_new_data_alert("Wykryto aktualizacje danych P3961")

        etl_success = self.run_etl()

        if etl_success:
            report_path = self.run_analysis()

            analyzer = DataAnalyzer(self.db)
            stats = analyzer.get_summary_stats()

            self.email.send_weekly_report(stats, report_path)

        print(f"\n[{datetime.now()}] TYGODNIOWE ZADANIE KONIEC\n")

    def daily_check(self):
        print(f"[{datetime.now()}] Codzienna kontrola...")

        has_updates = self.check_for_updates()

        if has_updates:
            self.email.send_new_data_alert("Wykryto nowe dane - uruchamiam ETL")
            self.run_etl()

    def start(self, weekly_day: str = "monday", weekly_time: str = "08:00",
              daily_time: str = "06:00"):
        """Standardowy tryb - raz dziennie + raz w tygodniu"""
        print(f"Scheduler uruchomiony (tryb STANDARDOWY)")
        print(f"  - Raport tygodniowy: {weekly_day} o {weekly_time}")
        print(f"  - Sprawdzanie aktualizacji: codziennie o {daily_time}")

        schedule.every().day.at(daily_time).do(self.daily_check)
        getattr(schedule.every(), weekly_day).at(weekly_time).do(self.weekly_job)

        while True:
            schedule.run_pending()
            time.sleep(60)

    def start_demo(self, check_interval_sec: int = 111, report_interval_sec: int = 10):
        """
        Tryb DEMO - czeste wykonywanie dla pokazu

        Args:
            check_interval_sec: Co ile sekund sprawdzac aktualizacje (default: 30s)
            report_interval_sec: Co ile sekund generowac raport (default: 120s = 2min)
        """
        print(f"  Sprawdzanie aktualizacji: co {check_interval_sec} sekund")
        print(f"  Generowanie raportu: co {report_interval_sec} sekund")

        schedule.every(check_interval_sec).seconds.do(self.daily_check)
        schedule.every(report_interval_sec).seconds.do(self.weekly_job)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def start_custom(self, check_minutes: int = 5, report_minutes: int = 30):
        """
        Tryb z konfigurowalnymi interwalami w minutach

        Args:
            check_minutes: Co ile minut sprawdzac aktualizacje
            report_minutes: Co ile minut generowac raport
        """
        print(f"  Sprawdzanie aktualizacji: co {check_minutes} minut")
        print(f"  Generowanie raportu: co {report_minutes} minut")

        schedule.every(check_minutes).minutes.do(self.daily_check)
        schedule.every(report_minutes).minutes.do(self.weekly_job)

        while True:
            schedule.run_pending()
            time.sleep(10)

    def run_now(self):
        self.weekly_job()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='GUS Analytics Scheduler')
    parser.add_argument('--run-now', action='store_true',
                        help='Uruchom pipeline natychmiast (bez schedulera)')
    parser.add_argument('--demo', action='store_true',
                        help='Tryb demo - czeste wykonywanie dla pokazu')
    parser.add_argument('--custom', action='store_true',
                        help='Tryb custom z konfigurowalnymi interwalami')
    parser.add_argument('--day', default='monday',
                        help='Dzien tygodnia dla trybu standardowego (default: monday)')
    parser.add_argument('--time', default='08:00',
                        help='Godzina dla trybu standardowego (default: 08:00)')
    parser.add_argument('--check-sec', type=int, default=30,
                        help='Interwal sprawdzania w sekundach dla demo (default: 30)')
    parser.add_argument('--report-sec', type=int, default=120,
                        help='Interwal raportu w sekundach dla demo (default: 120)')
    parser.add_argument('--check-min', type=int, default=5,
                        help='Interwal sprawdzania w minutach dla custom (default: 5)')
    parser.add_argument('--report-min', type=int, default=30,
                        help='Interwal raportu w minutach dla custom (default: 30)')

    args = parser.parse_args()

    scheduler = Scheduler()

    if args.run_now:
        scheduler.run_now()
    elif args.demo:
        scheduler.start_demo(
            check_interval_sec=args.check_sec,
            report_interval_sec=args.report_sec
        )
    elif args.custom:
        scheduler.start_custom(
            check_minutes=args.check_min,
            report_minutes=args.report_min
        )
    else:
        scheduler.start(weekly_day=args.day, weekly_time=args.time)


if __name__ == "__main__":
    main()