import schedule
import time
from datetime import datetime
from typing import Callable
import argparse
from pathlib import Path

from database import Database
from config import config
from api_client import GUSApiClient, GUSDataCache
from etl import ETLPipeline
from analysis import DataAnalyzer
from report import ReportGenerator, HTMLReportGenerator
from alerts import EmailAlert


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
                print("Wykryto nowe dane")
                self.cache.save(dataset)
                return True
            else:
                print("Brak nowych danych")
                return False

        except Exception as e:
            print(f"blad sprawdzania: {e}")
            return False

    def run_etl(self) -> bool:
        print(f"[{datetime.now()}] Uruchamianie ETL...")

        try:
            pipeline = ETLPipeline(self.db)
            result = pipeline.run(years=[2022, 2024], unit_level=2)

            if result.success:
                print(f"ETL sukces: {result.records_inserted} rekordów")
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
            self.email.send_new_data_alert("Wykryto aktualizację danych P3961")

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

    def start(self, weekly_day: str = "monday", weekly_time: str = "08:00"):
        print(f"Scheduler uruchomiony")
        print(f"  - Raport tygodniowy: {weekly_day} o {weekly_time}")
        print(f"  - Sprawdzanie aktualizacji: codziennie o 06:00")

        schedule.every().day.at("06:00").do(self.daily_check)

        getattr(schedule.every(), weekly_day).at(weekly_time).do(self.weekly_job)

        while True:
            schedule.run_pending()
            time.sleep(60)

    def run_now(self):
        self.weekly_job()


def main():
    parser = argparse.ArgumentParser(description='GUS Analytics Scheduler')
    parser.add_argument('--run-now', action='store_true', help='Uruchom natychmiast')
    parser.add_argument('--day', default='monday', help='Dzień tygodnia (default: monday)')
    parser.add_argument('--time', default='08:00', help='Godzina (default: 08:00)')

    args = parser.parse_args()

    scheduler = Scheduler()

    if args.run_now:
        scheduler.run_now()
    else:
        scheduler.start(weekly_day=args.day, weekly_time=args.time)


if __name__ == "__main__":
    main()