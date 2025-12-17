import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime
import os

from .config import config


@dataclass
class EmailConfig:
    smtp_host: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_user: str = os.getenv("SMTP_USER", "test")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    sender_email: str = os.getenv("SENDER_EMAIL", "")
    recipients: List[str] = None

    def __post_init__(self):
        if self.recipients is None:
            recipients_str = os.getenv("EMAIL_RECIPIENTS", "")
            self.recipients = [r.strip() for r in recipients_str.split(",") if r.strip()]


class EmailAlert:

    def __init__(self, email_config: EmailConfig = None):
        self.config = email_config or EmailConfig()

    def send(
            self,
            subject: str,
            body: str,
            recipients: List[str] = None,
            attachments: List[Path] = None,
            html: bool = False
    ) -> bool:
        if not self.config.smtp_user or not self.config.smtp_password:
            print("WARN: Email nie skonfigurowany (brak SMTP_USER/SMTP_PASSWORD)")
            return False

        recipients = recipients or self.config.recipients
        if not recipients:
            print("WARN: Brak odbiorców email")
            return False

        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.sender_email or self.config.smtp_user
            msg['To'] = ", ".join(recipients)
            msg['Subject'] = subject

            content_type = 'html' if html else 'plain'
            msg.attach(MIMEText(body, content_type, 'utf-8'))

            if attachments:
                for filepath in attachments:
                    if filepath.exists():
                        with open(filepath, 'rb') as f:
                            attachment = MIMEApplication(f.read())
                            attachment.add_header(
                                'Content-Disposition',
                                'attachment',
                                filename=filepath.name
                            )
                            msg.attach(attachment)

            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls()
                server.login(self.config.smtp_user, self.config.smtp_password)
                server.sendmail(
                    self.config.sender_email or self.config.smtp_user,
                    recipients,
                    msg.as_string()
                )

            print(f"Email wysłany do: {', '.join(recipients)}")
            return True

        except Exception as e:
            print(f"Błąd wysyłki email: {e}")
            return False

    def send_etl_success(
            self,
            records_count: int,
            duration: float,
            report_path: Path = None
    ) -> bool:
        subject = f"[GUS Analytics] ETL zakończony pomyślnie"

        body = f"""
        <h2>ETL Pipeline - Sukces</h2>
        <p>Pipeline ETL zakończył się pomyślnie.</p>

        <h3>Podsumowanie:</h3>
        <ul>
            <li><strong>Data:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</li>
            <li><strong>Załadowanych rekordów:</strong> {records_count:,}</li>
            <li><strong>Czas wykonania:</strong> {duration:.2f}s</li>
        </ul>

        <p>Raport w załączniku.</p>
        """

        attachments = [report_path] if report_path and report_path.exists() else None

        return self.send(subject, body, attachments=attachments, html=True)

    def send_etl_failure(self, error_message: str) -> bool:
        subject = f"[GUS Analytics] BŁĄD ETL"

        body = f"""
        <h2 style="color: red;">ETL Pipeline - Błąd</h2>
        <p>Pipeline ETL zakończył się błędem.</p>

        <h3>Szczegóły:</h3>
        <ul>
            <li><strong>Data:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</li>
            <li><strong>Błąd:</strong> {error_message}</li>
        </ul>

        <p>Wymagana interwencja.</p>
        """

        return self.send(subject, body, html=True)

    def send_new_data_alert(self, changes_summary: str) -> bool:
        subject = f"[GUS Analytics] Nowe dane GUS"

        body = f"""
        <h2>Wykryto nowe dane</h2>
        <p>System wykrył aktualizację danych w API GUS.</p>

        <h3>Zmiany:</h3>
        <pre>{changes_summary}</pre>

        <p>ETL zostanie uruchomiony automatycznie.</p>
        """

        return self.send(subject, body, html=True)

    def send_anomaly_alert(self, anomalies: List[dict]) -> bool:
        subject = f"[GUS Analytics] Wykryto anomalie w danych"

        anomaly_rows = ""
        for a in anomalies[:10]:
            anomaly_rows += f"<tr><td>{a.get('jednostka', '')}</td><td>{a.get('zmiana_pct', 0):.1f}%</td></tr>"

        body = f"""
        <h2 style="color: orange;">Wykryto anomalie</h2>
        <p>System wykrył nietypowe zmiany w danych.</p>

        <h3>Anomalie ({len(anomalies)} jednostek):</h3>
        <table border="1" cellpadding="5">
            <tr><th>Jednostka</th><th>Zmiana</th></tr>
            {anomaly_rows}
        </table>

        <p>Zalecana weryfikacja manualna.</p>
        """

        return self.send(subject, body, html=True)

    def send_weekly_report(
            self,
            summary_stats: dict,
            report_path: Path = None
    ) -> bool:
        subject = f"[GUS Analytics] Raport tygodniowy - {datetime.now().strftime('%Y-%m-%d')}"

        body = f"""
        <h2>Raport tygodniowy GUS Analytics</h2>

        <h3>Statystyki:</h3>
        <ul>
            <li><strong>Rekordów w bazie:</strong> {summary_stats.get('total_records', 0):,}</li>
            <li><strong>Województw:</strong> {summary_stats.get('regions_count', 0)}</li>
            <li><strong>Suma kosztów:</strong> {summary_stats.get('total_value', 0) / 1000:,.1f} mln zł</li>
        </ul>

        <p>Pełny raport w załączniku.</p>
        """

        attachments = [report_path] if report_path and report_path.exists() else None

        return self.send(subject, body, attachments=attachments, html=True)