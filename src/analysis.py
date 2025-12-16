import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

from .database import Database, DatabaseQueries
from .config import config

@dataclass
class AnalysisResult:
    name: str
    description: str
    data: pd.DataFrame
    chart_static: Optional[Path] = None
    chart_interactive: Optional[str] = None
    insights: List[str] = None


class DataAnalyzer:

    def __init__(self, db: Database):
        self.db = db
        self.queries = DatabaseQueries(db)
        self.output_dir = config.paths.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        plt.style.use('seaborn-v0_8-whitegrid')
        plt.rcParams['font.size'] = 10
        plt.rcParams['figure.figsize'] = (12, 6)

    def run_all_analyses(self) -> List[AnalysisResult]:
        results = []

        print("1. Analiza trendów czasowych...")
        results.append(self.analyze_trends())

        print("2. Analiza regionalna...")
        results.append(self.analyze_regions())

        print("3. Analiza struktury kosztów...")
        results.append(self.analyze_cost_structure())

        print("4. Analiza anomalii...")
        results.append(self.analyze_anomalies())

        print("5. Analiza dynamiki zmian...")
        results.append(self.analyze_dynamics())

        return results

    def _get_data(self) -> pd.DataFrame:
        sql = f"""
            SELECT 
                j.kod_gus,
                j.nazwa as jednostka,
                j.poziom,
                t.kod as typ_kosztu,
                t.nazwa as typ_kosztu_nazwa,
                t.kategoria,
                o.rok,
                f.wartosc
            FROM {self.db.schema}.fact_koszty f
            JOIN {self.db.schema}.dim_jednostka j ON f.jednostka_id = j.id
            JOIN {self.db.schema}.dim_typ_kosztu t ON f.typ_kosztu_id = t.id
            JOIN {self.db.schema}.dim_okres o ON f.okres_id = o.id
        """
        data = self.db.fetch_all(sql)
        df = pd.DataFrame(data)

        # --- FIX START ---
        # Convert Decimal objects from database to standard floats for analysis
        if 'wartosc' in df.columns:
            df['wartosc'] = df['wartosc'].astype(float)

        # Optional: Ensure year is integer
        if 'rok' in df.columns:
            df['rok'] = df['rok'].astype(int)
        # --- FIX END ---

        return df

    def analyze_trends(self) -> AnalysisResult:
        df = self._get_data()

        trends = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['rok', 'kategoria'])['wartosc'].sum().reset_index()
        trends = trends.sort_values(['kategoria', 'rok'])

        fig, ax = plt.subplots(figsize=(12, 6))

        for kategoria in trends['kategoria'].unique():
            data = trends[trends['kategoria'] == kategoria]
            ax.plot(data['rok'].astype(int), data['wartosc'] / 1000, marker='o', linewidth=2, label=kategoria)

        ax.set_xlabel('Rok')
        ax.set_ylabel('Koszty (mln zł)')
        ax.set_title('Trend kosztów utrzymania wg kategorii')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        static_path = self.output_dir / 'trend_czasowy.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        plot_data = trends.copy()
        plot_data['rok_str'] = plot_data['rok'].astype(int).astype(str)

        fig_plotly = px.bar(
            plot_data,
            x='rok_str',
            y='wartosc',
            color='kategoria',
            barmode='group',
            title='Trend kosztów utrzymania wg kategorii',
            labels={'rok_str': 'Rok', 'wartosc': 'Koszty (tys. zł)', 'kategoria': 'Kategoria'}
        )
        fig_plotly.update_xaxes(type='category')
        interactive_html = fig_plotly.to_html(full_html=False)

        total_by_year = df[df['poziom'] == 'WOJEWODZTWO'].groupby('rok')['wartosc'].sum()
        if len(total_by_year) >= 2:
            first_year = total_by_year.iloc[0]
            last_year = total_by_year.iloc[-1]
            change_pct = ((last_year - first_year) / first_year) * 100

            insights = [
                f"Całkowite koszty zmieniły się o {change_pct:.1f}% w analizowanym okresie",
                f"Koszty w pierwszym roku: {first_year / 1000:.1f} mln zł",
                f"Koszty w ostatnim roku: {last_year / 1000:.1f} mln zł"
            ]
        else:
            insights = ["Niewystarczające dane do analizy trendu"]

        return AnalysisResult(
            name="Trendy czasowe",
            description="Analiza zmian kosztów w czasie wg kategorii",
            data=trends,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_regions(self) -> AnalysisResult:
        df = self._get_data()

        regions = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'rok'])['wartosc'].sum().reset_index()

        latest_year = regions['rok'].max()
        latest = regions[regions['rok'] == latest_year].sort_values('wartosc', ascending=True)

        fig, ax = plt.subplots(figsize=(12, 8))

        colors = plt.cm.RdYlGn(range(0, 256, 256 // len(latest)))
        ax.barh(latest['jednostka'], latest['wartosc'] / 1000, color=colors)

        ax.set_xlabel('Koszty (mln zł)')
        ax.set_title(f'Koszty utrzymania wg województw ({latest_year})')

        for i, (_, row) in enumerate(latest.iterrows()):
            ax.text(row['wartosc'] / 1000 + 0.5, i, f"{row['wartosc'] / 1000:.1f}", va='center', fontsize=9)

        static_path = self.output_dir / 'porownanie_regionow.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.bar(
            regions,
            x='wartosc',
            y='jednostka',
            color='rok',
            orientation='h',
            title='Koszty utrzymania wg województw',
            labels={'wartosc': 'Koszty (tys. zł)', 'jednostka': 'Województwo', 'rok': 'Rok'},
            barmode='group'
        )
        fig_plotly.update_layout(height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        top3 = latest.nlargest(3, 'wartosc')['jednostka'].tolist()
        bottom3 = latest.nsmallest(3, 'wartosc')['jednostka'].tolist()

        insights = [
            f"Najwyższe koszty: {', '.join(top3)}",
            f"Najniższe koszty: {', '.join(bottom3)}",
            f"Rozstęp: {latest['wartosc'].max() / 1000:.1f} - {latest['wartosc'].min() / 1000:.1f} mln zł"
        ]

        return AnalysisResult(
            name="Analiza regionalna",
            description="Porównanie kosztów między województwami",
            data=regions,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_cost_structure(self) -> AnalysisResult:
        df = self._get_data()

        structure = df[
            df['poziom'] == 'WOJEWODZTWO'
            ].groupby(['rok', 'kategoria'])['wartosc'].sum().reset_index()

        total_by_year = structure.groupby('rok')['wartosc'].transform('sum')
        structure['udzial'] = (structure['wartosc'] / total_by_year * 100).round(1)

        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        pivot = structure.pivot(index='rok', columns='kategoria', values='wartosc')
        pivot.plot(kind='bar', stacked=True, ax=axes[0], colormap='Set2')
        axes[0].set_title('Struktura kosztów wg roku')
        axes[0].set_xlabel('Rok')
        axes[0].set_ylabel('Koszty (tys. zł)')
        axes[0].legend(title='Kategoria')
        axes[0].tick_params(axis='x', rotation=0)

        latest_year = structure['rok'].max()
        latest_struct = structure[structure['rok'] == latest_year]
        axes[1].pie(
            latest_struct['wartosc'],
            labels=latest_struct['kategoria'],
            autopct='%1.1f%%',
            colors=plt.cm.Set2.colors
        )
        axes[1].set_title(f'Struktura kosztów ({latest_year})')

        plt.tight_layout()
        static_path = self.output_dir / 'struktura_kosztow.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.sunburst(
            structure,
            path=['rok', 'kategoria'],
            values='wartosc',
            title='Struktura kosztów - interaktywna'
        )
        interactive_html = fig_plotly.to_html(full_html=False)

        latest_struct = structure[structure['rok'] == latest_year]
        dominant = latest_struct.loc[latest_struct['wartosc'].idxmax(), 'kategoria']
        dominant_pct = latest_struct.loc[latest_struct['wartosc'].idxmax(), 'udzial']

        insights = [
            f"Dominująca kategoria: {dominant} ({dominant_pct}%)",
            f"Liczba kategorii: {structure['kategoria'].nunique()}",
        ]

        return AnalysisResult(
            name="Struktura kosztów",
            description="Rozkład kosztów między kategoriami",
            data=structure,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_anomalies(self) -> AnalysisResult:
        df = self._get_data()

        yearly = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'rok'])['wartosc'].sum().reset_index()

        pivot = yearly.pivot(index='jednostka', columns='rok', values='wartosc')

        if pivot.shape[1] >= 2:
            first_col = pivot.columns[0]
            last_col = pivot.columns[-1]
            pivot['zmiana_pct'] = ((pivot[last_col] - pivot[first_col]) / pivot[first_col] * 100).round(1)
            pivot['zmiana_abs'] = (pivot[last_col] - pivot[first_col]).round(1)

            anomalies = pivot[['zmiana_pct', 'zmiana_abs']].reset_index()
            anomalies = anomalies.sort_values('zmiana_pct', ascending=False)
        else:
            anomalies = pd.DataFrame(columns=['jednostka', 'zmiana_pct', 'zmiana_abs'])

        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        colors = ['green' if x > 0 else 'red' for x in anomalies['zmiana_pct']]
        axes[0].barh(anomalies['jednostka'], anomalies['zmiana_pct'], color=colors)
        axes[0].set_xlabel('Zmiana (%)')
        axes[0].set_title('Zmiana kosztów (%) - pierwszy vs ostatni rok')
        axes[0].axvline(x=0, color='black', linewidth=0.5)

        mean_change = anomalies['zmiana_pct'].mean()
        std_change = anomalies['zmiana_pct'].std()

        outliers = anomalies[
            (anomalies['zmiana_pct'] > mean_change + 2 * std_change) |
            (anomalies['zmiana_pct'] < mean_change - 2 * std_change)
            ]

        axes[1].scatter(anomalies['zmiana_abs'], anomalies['zmiana_pct'], alpha=0.6)
        axes[1].axhline(y=mean_change, color='gray', linestyle='--', label=f'Średnia: {mean_change:.1f}%')
        axes[1].axhline(y=mean_change + 2 * std_change, color='red', linestyle=':', label='+2 std')
        axes[1].axhline(y=mean_change - 2 * std_change, color='red', linestyle=':', label='-2 std')

        for _, row in outliers.iterrows():
            axes[1].annotate(row['jednostka'], (row['zmiana_abs'], row['zmiana_pct']), fontsize=8)

        axes[1].set_xlabel('Zmiana absolutna (tys. zł)')
        axes[1].set_ylabel('Zmiana (%)')
        axes[1].set_title('Wykrywanie anomalii')
        axes[1].legend()

        plt.tight_layout()
        static_path = self.output_dir / 'anomalie.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.bar(
            anomalies,
            x='zmiana_pct',
            y='jednostka',
            orientation='h',
            color='zmiana_pct',
            color_continuous_scale='RdYlGn',
            title='Zmiana kosztów wg województw'
        )
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = [
            f"Średnia zmiana: {mean_change:.1f}%",
            f"Odchylenie std: {std_change:.1f}%",
            f"Wykryte anomalie: {len(outliers)} jednostek"
        ]

        if len(outliers) > 0:
            insights.append(f"Anomalie: {', '.join(outliers['jednostka'].tolist())}")

        return AnalysisResult(
            name="Analiza anomalii",
            description="Wykrywanie nietypowych zmian kosztów",
            data=anomalies,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_dynamics(self) -> AnalysisResult:
        df = self._get_data()

        dynamics = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['rok', 'kategoria'])['wartosc'].sum().reset_index()
        dynamics = dynamics.sort_values(['kategoria', 'rok'])

        dynamics['zmiana_rr'] = dynamics.groupby('kategoria')['wartosc'].pct_change() * 100

        fig, ax = plt.subplots(figsize=(12, 6))

        for kategoria in dynamics['kategoria'].unique():
            data = dynamics[dynamics['kategoria'] == kategoria]
            ax.plot(data['rok'].astype(int), data['zmiana_rr'], marker='s', linewidth=2, label=kategoria)

        ax.axhline(y=0, color='black', linewidth=0.5)
        ax.set_xlabel('Rok')
        ax.set_ylabel('Zmiana r/r (%)')
        ax.set_title('Dynamika zmian kosztów rok do roku')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        static_path = self.output_dir / 'dynamika_zmian.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        plot_data = dynamics.dropna().copy()
        plot_data['rok_str'] = plot_data['rok'].astype(int).astype(str)

        fig_plotly = px.bar(
            plot_data,
            x='rok_str',
            y='zmiana_rr',
            color='kategoria',
            barmode='group',
            title='Dynamika zmian r/r',
            labels={'rok_str': 'Rok', 'zmiana_rr': 'Zmiana r/r (%)', 'kategoria': 'Kategoria'}
        )
        fig_plotly.update_xaxes(type='category')
        interactive_html = fig_plotly.to_html(full_html=False)

        latest = dynamics[dynamics['rok'] == dynamics['rok'].max()]
        fastest = latest.loc[latest['zmiana_rr'].idxmax()] if not latest['zmiana_rr'].isna().all() else None
        slowest = latest.loc[latest['zmiana_rr'].idxmin()] if not latest['zmiana_rr'].isna().all() else None

        insights = []
        if fastest is not None:
            insights.append(f"Najszybszy wzrost: {fastest['kategoria']} ({fastest['zmiana_rr']:.1f}%)")
        if slowest is not None:
            insights.append(f"Najwolniejszy wzrost: {slowest['kategoria']} ({slowest['zmiana_rr']:.1f}%)")

        return AnalysisResult(
            name="Dynamika zmian",
            description="Analiza zmian rok do roku",
            data=dynamics,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def get_summary_stats(self) -> Dict:
        df = self._get_data()

        return {
            'total_records': len(df),
            'years': sorted(df['rok'].unique().tolist()),
            'regions_count': df[df['poziom'] == 'WOJEWODZTWO']['jednostka'].nunique(),
            'categories': df['kategoria'].unique().tolist(),
            'total_value': df[df['poziom'] == 'WOJEWODZTWO']['wartosc'].sum(),
            'avg_value': df[df['poziom'] == 'WOJEWODZTWO']['wartosc'].mean(),
            'min_value': df['wartosc'].min(),
            'max_value': df['wartosc'].max()
        }