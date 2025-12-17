import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field

from .database import Database, DatabaseQueries
from .config import config


@dataclass
class AnalysisResult:
    name: str
    description: str
    data: pd.DataFrame
    chart_static: Optional[Path] = None
    chart_interactive: Optional[str] = None
    insights: List[str] = field(default_factory=list)


class DataAnalyzer:
    TYP_LABELS = {
        'ZASOBY_GMINNE': 'Gminne',
        'ZASOBY_SKARBU_PANSTWA': 'Skarb Panstwa',
        'ZASOBY_SPOLDZIELNI': 'Spoldzielnie',
        'ZASOBY_TBS': 'TBS',
        'ZASOBY_WSPOLNOTY': 'Wspolnoty',
        'ZASOBY_INNE': 'Inne podmioty',
        'ZASOBY_ZAKLADY_PRACY': 'Zaklady pracy'
    }

    COLORS_7 = {
        'ZASOBY_GMINNE': '#2ecc71',
        'ZASOBY_SKARBU_PANSTWA': '#27ae60',
        'ZASOBY_SPOLDZIELNI': '#3498db',
        'ZASOBY_TBS': '#9b59b6',
        'ZASOBY_WSPOLNOTY': '#e74c3c',
        'ZASOBY_INNE': '#e67e22',
        'ZASOBY_ZAKLADY_PRACY': '#f39c12'
    }

    def __init__(self, db: Database):
        self.db = db
        self.queries = DatabaseQueries(db)
        self.output_dir = config.paths.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        plt.style.use('seaborn-v0_8-whitegrid')
        plt.rcParams['font.size'] = 10
        plt.rcParams['figure.figsize'] = (14, 8)
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 11

    def run_all_analyses(self) -> List[AnalysisResult]:
        results = []

        results.append(self.analyze_trends())
        results.append(self.analyze_dynamics())
        results.append(self.analyze_ranking())
        results.append(self.analyze_correlations())
        results.append(self.analyze_volatility())
        results.append(self.analyze_owner_comparison())
        results.append(self.analyze_summary_statistics())

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
            ORDER BY o.rok, j.nazwa
        """
        data = self.db.fetch_all(sql)
        df = pd.DataFrame(data)
        df['typ_label'] = df['typ_kosztu'].map(self.TYP_LABELS)

        if 'wartosc' in df.columns:
            df['wartosc'] = df['wartosc'].astype(float)

        if 'rok' in df.columns:
            df['rok'] = df['rok'].astype(int)

        return df

    def analyze_trends(self) -> AnalysisResult:
        df = self._get_data()

        trends = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['rok', 'typ_kosztu'])['wartosc'].sum().reset_index()
        trends = trends.sort_values(['typ_kosztu', 'rok'])
        trends['typ_label'] = trends['typ_kosztu'].map(self.TYP_LABELS)

        fig, ax = plt.subplots(figsize=(16, 8))

        for typ in sorted(trends['typ_kosztu'].unique()):
            data = trends[trends['typ_kosztu'] == typ]
            color = self.COLORS_7.get(typ, '#95a5a6')
            label = self.TYP_LABELS.get(typ, typ)
            ax.plot(data['rok'].astype(int), data['wartosc'] / 1000,
                    marker='o', linewidth=2.5, markersize=8, label=label, color=color)

        ax.set_xlabel('Rok')
        ax.set_ylabel('Koszty (mln zl)')
        ax.set_title('Trend kosztow utrzymania wg typow wlascicieli')
        ax.legend(loc='upper left', fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        plt.tight_layout()
        static_path = self.output_dir / 'trend_czasowy.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        plot_data = trends.copy()
        plot_data['rok_str'] = plot_data['rok'].astype(int).astype(str)

        fig_plotly = px.line(
            plot_data,
            x='rok_str',
            y='wartosc',
            color='typ_label',
            markers=True,
            title='Trend kosztow utrzymania wg typow wlascicieli',
            labels={'rok_str': 'Rok', 'wartosc': 'Koszty (tys. zl)', 'typ_label': 'Typ wlasciciela'}
        )
        fig_plotly.update_xaxes(type='category')
        fig_plotly.update_layout(hovermode='x unified', height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        total_by_year = df[df['poziom'] == 'WOJEWODZTWO'].groupby('rok')['wartosc'].sum().sort_index()
        years = sorted(total_by_year.index.tolist())

        insights = []
        if len(total_by_year) >= 2:
            first_val = total_by_year.iloc[0]
            last_val = total_by_year.iloc[-1]
            change_pct = ((last_val - first_val) / first_val) * 100
            insights.append(
                f"Calkowite koszty zmienily sie o {change_pct:.1f}% w okresie {int(years[0])}-{int(years[-1])}")

            latest = trends[trends['rok'] == years[-1]].sort_values('wartosc', ascending=False)
            top3 = latest.head(3)
            for _, row in top3.iterrows():
                insights.append(f"Top: {row['typ_label']} - {row['wartosc'] / 1000:.1f} mln zl")

        return AnalysisResult(
            name="Trendy czasowe",
            description="xyz",
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

        fig, axes = plt.subplots(1, 2, figsize=(18, 9))

        n_regions = len(latest)
        colors = plt.cm.RdYlGn(np.linspace(0, 1, n_regions)) if n_regions > 0 else ['gray']
        bars = axes[0].barh(latest['jednostka'], latest['wartosc'] / 1000, color=colors)
        axes[0].set_xlabel('Koszty (mln zl)')
        axes[0].set_title(f'Calkowite koszty wg wojewodztw ({int(latest_year)})')

        for bar, (_, row) in zip(bars, latest.iterrows()):
            axes[0].text(row['wartosc'] / 1000 + 0.3, bar.get_y() + bar.get_height() / 2,
                         f"{row['wartosc'] / 1000:.1f}", va='center', fontsize=9)

        region_typ = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'typ_kosztu'])[
            'wartosc'].sum().reset_index()
        region_pivot = region_typ.pivot(index='jednostka', columns='typ_kosztu', values='wartosc').fillna(0)
        region_pivot = region_pivot.loc[latest['jednostka']]

        region_pivot_renamed = region_pivot.rename(columns=self.TYP_LABELS)
        region_pivot_renamed.plot(kind='barh', stacked=True, ax=axes[1], colormap='Set2', width=0.8)
        axes[1].set_xlabel('Koszty (tys. zl)')
        axes[1].set_title('Struktura kosztow wg wojewodztw i typow')
        axes[1].legend(title='Typ', bbox_to_anchor=(1.02, 1), fontsize=8)

        plt.tight_layout()
        static_path = self.output_dir / 'porownanie_regionow.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.bar(
            region_typ,
            x='wartosc',
            y='jednostka',
            color='typ_kosztu',
            orientation='h',
            title='Koszty wg wojewodztw i typow wlascicieli',
            labels={'wartosc': 'Koszty (tys. zl)', 'jednostka': 'Wojewodztwo', 'typ_kosztu': 'Typ'},
            barmode='stack'
        )
        fig_plotly.update_layout(height=700)
        interactive_html = fig_plotly.to_html(full_html=False)

        top3 = latest.nlargest(3, 'wartosc')['jednostka'].tolist()
        bottom3 = latest.nsmallest(3, 'wartosc')['jednostka'].tolist()

        insights = [
            f"Najwyzsze koszty ({int(latest_year)}): {', '.join(top3)}",
            f"Najnizsze koszty ({int(latest_year)}): {', '.join(bottom3)}",
            f"Rozpietosc: {latest['wartosc'].max() / 1000:.1f} - {latest['wartosc'].min() / 1000:.1f} mln zl",
            f"Srednia wojewodztw: {latest['wartosc'].mean() / 1000:.1f} mln zl"
        ]

        return AnalysisResult(
            name="Analiza regionalna",
            description="Porownanie kosztow utrzymania miedzy wojewodztwami",
            data=regions,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_cost_structure(self) -> AnalysisResult:
        df = self._get_data()

        structure = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['rok', 'typ_kosztu'])['wartosc'].sum().reset_index()
        structure['typ_label'] = structure['typ_kosztu'].map(self.TYP_LABELS)

        total_by_year = structure.groupby('rok')['wartosc'].transform('sum')
        structure['udzial'] = (structure['wartosc'] / total_by_year * 100).round(2)

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        pivot = structure.pivot(index='rok', columns='typ_label', values='wartosc')
        pivot.plot(kind='bar', stacked=True, ax=axes[0, 0], colormap='Set2', width=0.7)
        axes[0, 0].set_title('Struktura kosztow wg roku (wartosci)')
        axes[0, 0].set_xlabel('Rok')
        axes[0, 0].set_ylabel('Koszty (tys. zl)')
        axes[0, 0].legend(title='Typ', fontsize=8, loc='upper left')
        axes[0, 0].tick_params(axis='x', rotation=0)

        pivot_pct = structure.pivot(index='rok', columns='typ_label', values='udzial')
        pivot_pct.plot(kind='bar', stacked=True, ax=axes[0, 1], colormap='Set2', width=0.7)
        axes[0, 1].set_title('Struktura kosztow wg roku (udzialy %)')
        axes[0, 1].set_xlabel('Rok')
        axes[0, 1].set_ylabel('Udzial (%)')
        axes[0, 1].legend(title='Typ', fontsize=8, loc='upper left')
        axes[0, 1].tick_params(axis='x', rotation=0)
        axes[0, 1].set_ylim(0, 100)

        latest_year = structure['rok'].max()
        latest_struct = structure[structure['rok'] == latest_year].sort_values('udzial', ascending=False)

        colors_pie = [self.COLORS_7.get(t, '#95a5a6') for t in latest_struct['typ_kosztu']]
        wedges, texts, autotexts = axes[1, 0].pie(
            latest_struct['wartosc'],
            labels=latest_struct['typ_label'],
            autopct='%1.1f%%',
            colors=colors_pie,
            explode=[0.02] * len(latest_struct)
        )
        axes[1, 0].set_title(f'Struktura kosztow ({int(latest_year)})')

        typ_totals = structure.groupby('typ_label')['wartosc'].sum().sort_values(ascending=True)
        colors_bar = [self.COLORS_7.get(k, '#95a5a6') for k in
                      [k for k, v in self.TYP_LABELS.items() if v in typ_totals.index]]
        axes[1, 1].barh(typ_totals.index, typ_totals.values / 1000)
        axes[1, 1].set_xlabel('Suma kosztow (mln zl)')
        axes[1, 1].set_title('Laczne koszty wg typu (wszystkie lata)')

        for i, (idx, val) in enumerate(typ_totals.items()):
            axes[1, 1].text(val / 1000 + 0.5, i, f'{val / 1000:.1f}', va='center', fontsize=9)

        plt.tight_layout()
        static_path = self.output_dir / 'struktura_kosztow.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.sunburst(
            structure,
            path=['rok', 'typ_label'],
            values='wartosc',
            title='Struktura kosztow',
            color='typ_label'
        )
        fig_plotly.update_layout(height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = []
        for _, row in latest_struct.iterrows():
            insights.append(f"{row['typ_label']}: {row['udzial']:.1f}% ({row['wartosc'] / 1000:.1f} mln zl)")

        return AnalysisResult(
            name="Struktura kosztow",
            description="Rozklad kosztow miedzy typami wlascicieli",
            data=structure,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_dynamics(self) -> AnalysisResult:
        df = self._get_data()

        dynamics = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['rok', 'typ_kosztu'])['wartosc'].sum().reset_index()
        dynamics = dynamics.sort_values(['typ_kosztu', 'rok'])
        dynamics['typ_label'] = dynamics['typ_kosztu'].map(self.TYP_LABELS)

        dynamics['wartosc_poprzedni'] = dynamics.groupby('typ_kosztu')['wartosc'].shift(1)
        dynamics['zmiana_rr'] = (
                    (dynamics['wartosc'] - dynamics['wartosc_poprzedni']) / dynamics['wartosc_poprzedni'] * 100).round(
            2)
        dynamics['zmiana_abs'] = dynamics['wartosc'] - dynamics['wartosc_poprzedni']

        dynamics_clean = dynamics.dropna(subset=['zmiana_rr'])

        fig, axes = plt.subplots(1, 2, figsize=(18, 8))

        for typ in sorted(dynamics_clean['typ_kosztu'].unique()):
            data = dynamics_clean[dynamics_clean['typ_kosztu'] == typ]
            color = self.COLORS_7.get(typ, '#95a5a6')
            label = self.TYP_LABELS.get(typ, typ)
            axes[0].plot(data['rok'].astype(int), data['zmiana_rr'],
                         marker='s', linewidth=2, markersize=8, label=label, color=color)

        axes[0].axhline(y=0, color='black', linewidth=1, linestyle='--')
        axes[0].set_xlabel('Rok')
        axes[0].set_ylabel('Zmiana r/r (%)')
        axes[0].set_title('Dynamika zmian kosztow rok do roku')
        axes[0].legend(fontsize=8)
        axes[0].grid(True, alpha=0.3)
        axes[0].xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        pivot = dynamics_clean.pivot(index='rok', columns='typ_label', values='zmiana_rr')
        pivot.plot(kind='bar', ax=axes[1], width=0.8)
        axes[1].axhline(y=0, color='black', linewidth=1, linestyle='--')
        axes[1].set_xlabel('Rok')
        axes[1].set_ylabel('Zmiana r/r (%)')
        axes[1].set_title('Zmiana procentowa wg typu i roku')
        axes[1].legend(title='Typ', fontsize=8)
        axes[1].tick_params(axis='x', rotation=0)

        plt.tight_layout()
        static_path = self.output_dir / 'dynamika_zmian.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        plot_data = dynamics_clean.copy()
        plot_data['rok_str'] = plot_data['rok'].astype(int).astype(str)

        fig_plotly = px.bar(
            plot_data,
            x='rok_str',
            y='zmiana_rr',
            color='typ_label',
            barmode='group',
            title='Dynamika zmian r/r',
            labels={'rok_str': 'Rok', 'zmiana_rr': 'Zmiana r/r (%)', 'typ_label': 'Typ'},
            text='zmiana_rr'
        )
        fig_plotly.update_xaxes(type='category')
        fig_plotly.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig_plotly.update_layout(height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = []
        latest_year = dynamics_clean['rok'].max()
        latest = dynamics_clean[dynamics_clean['rok'] == latest_year].sort_values('zmiana_rr', ascending=False)

        if not latest.empty:
            fastest = latest.iloc[0]
            slowest = latest.iloc[-1]
            insights.append(
                f"Najszybszy wzrost w {int(latest_year)}: {fastest['typ_label']} ({fastest['zmiana_rr']:.1f}%)")
            insights.append(
                f"Najwolniejszy/spadek w {int(latest_year)}: {slowest['typ_label']} ({slowest['zmiana_rr']:.1f}%)")

        avg_by_typ = dynamics_clean.groupby('typ_label')['zmiana_rr'].mean().sort_values(ascending=False)
        insights.append("Srednie roczne zmiany:")
        for typ, avg in avg_by_typ.items():
            insights.append(f"  {typ}: {avg:.1f}%")

        return AnalysisResult(
            name="Dynamika zmian",
            description="Analiza zmian kosztow rok do roku dla kazdego typu wlasciciela",
            data=dynamics,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_anomalies(self) -> AnalysisResult:
        df = self._get_data()

        yearly = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'rok'])['wartosc'].sum().reset_index()

        pivot = yearly.pivot(index='jednostka', columns='rok', values='wartosc')
        years = sorted(pivot.columns)

        first_year = years[0] if years else 0
        last_year = years[-1] if years else 0

        if len(years) >= 2:
            pivot['zmiana_pct'] = ((pivot[last_year] - pivot[first_year]) / pivot[first_year] * 100).round(1)
            pivot['zmiana_abs'] = (pivot[last_year] - pivot[first_year]).round(1)
        else:
            pivot['zmiana_pct'] = 0
            pivot['zmiana_abs'] = 0

        anomalies = pivot[['zmiana_pct', 'zmiana_abs']].reset_index()
        anomalies = anomalies.sort_values('zmiana_pct', ascending=False)

        mean_change = anomalies['zmiana_pct'].mean()
        std_change = anomalies['zmiana_pct'].std() if len(anomalies) > 1 else 0

        anomalies['is_outlier'] = (
                (anomalies['zmiana_pct'] > mean_change + 2 * std_change) |
                (anomalies['zmiana_pct'] < mean_change - 2 * std_change)
        ) if std_change > 0 else False

        fig, axes = plt.subplots(1, 2, figsize=(18, 8))

        colors_list = ['#e74c3c' if x else '#3498db' for x in anomalies['is_outlier']]
        bars = axes[0].barh(anomalies['jednostka'], anomalies['zmiana_pct'], color=colors_list)
        axes[0].axvline(x=mean_change, color='green', linewidth=2, linestyle='--', label=f'Srednia: {mean_change:.1f}%')
        if std_change > 0:
            axes[0].axvline(x=mean_change + 2 * std_change, color='orange', linewidth=1, linestyle=':')
            axes[0].axvline(x=mean_change - 2 * std_change, color='orange', linewidth=1, linestyle=':')
        axes[0].set_xlabel(f'Zmiana {int(first_year)}-{int(last_year)} (%)')
        axes[0].set_title('Zmiana kosztow wg wojewodztw (czerwone = anomalie)')
        axes[0].legend(loc='lower right')

        axes[1].scatter(anomalies['zmiana_abs'] / 1000, anomalies['zmiana_pct'],
                        c=colors_list, s=100, alpha=0.7)
        axes[1].axhline(y=mean_change, color='green', linestyle='--', label='Srednia')

        for _, row in anomalies.iterrows():
            axes[1].annotate(row['jednostka'][:12],
                             (row['zmiana_abs'] / 1000, row['zmiana_pct']),
                             fontsize=8, alpha=0.7)

        axes[1].set_xlabel('Zmiana absolutna (mln zl)')
        axes[1].set_ylabel('Zmiana (%)')
        axes[1].set_title('Wykrywanie anomalii - rozklad zmian')

        plt.tight_layout()
        static_path = self.output_dir / 'anomalie.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.scatter(
            anomalies,
            x='zmiana_abs',
            y='zmiana_pct',
            color='is_outlier',
            hover_name='jednostka',
            size=abs(anomalies['zmiana_pct']),
            title='Wykrywanie anomalii',
            labels={'zmiana_abs': 'Zmiana absolutna (tys. zl)', 'zmiana_pct': 'Zmiana (%)', 'is_outlier': 'Anomalia'}
        )
        fig_plotly.update_layout(height=500)
        interactive_html = fig_plotly.to_html(full_html=False)

        outliers = anomalies[anomalies['is_outlier']]
        insights = [
            f"Okres analizy: {int(first_year)}-{int(last_year)}",
            f"Srednia zmiana: {mean_change:.1f}%",
            f"Odchylenie standardowe: {std_change:.1f}%",
            f"Wykryte anomalie: {len(outliers)} wojewodztw"
        ]
        if len(outliers) > 0:
            for _, row in outliers.iterrows():
                insights.append(f"  - {row['jednostka']}: {row['zmiana_pct']:.1f}%")

        return AnalysisResult(
            name="Analiza anomalii",
            description=f"Wykrywanie nietypowych zmian kosztow miedzy wojewodztwami",
            data=anomalies,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_ranking(self) -> AnalysisResult:
        df = self._get_data()

        ranking = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'rok']).agg({
            'wartosc': 'sum'
        }).reset_index()

        ranking_pivot = ranking.pivot(index='jednostka', columns='rok', values='wartosc')

        for year in ranking_pivot.columns:
            ranking_pivot[f'rank_{int(year)}'] = ranking_pivot[year].rank(ascending=False).astype(int)

        rank_cols = [c for c in ranking_pivot.columns if str(c).startswith('rank_')]

        fig, ax = plt.subplots(figsize=(16, 10))

        cmap = plt.cm.get_cmap('tab20')

        for i, (idx, row) in enumerate(ranking_pivot.iterrows()):
            years_list = [int(str(c).replace('rank_', '')) for c in rank_cols]
            ranks = [row[c] for c in rank_cols]
            ax.plot(years_list, ranks, marker='o', linewidth=2.5, markersize=10,
                    label=idx, color=cmap(i % 20))
            ax.annotate(idx[:8], (years_list[-1], ranks[-1]),
                        textcoords="offset points", xytext=(5, 0), fontsize=8)

        ax.set_xlabel('Rok')
        ax.set_ylabel('Pozycja w rankingu (1 = najwyzsze koszty)')
        ax.set_title('Zmiana pozycji wojewodztw w rankingu kosztow')
        ax.invert_yaxis()
        ax.set_yticks(range(1, len(ranking_pivot) + 1))
        ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        plt.tight_layout()
        static_path = self.output_dir / 'ranking.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        rank_data = []
        for idx, row in ranking_pivot.iterrows():
            for c in rank_cols:
                year = int(str(c).replace('rank_', ''))
                rank_data.append({'jednostka': idx, 'rok': year, 'rank': row[c]})

        rank_df = pd.DataFrame(rank_data)

        fig_plotly = px.line(
            rank_df,
            x='rok',
            y='rank',
            color='jednostka',
            markers=True,
            title='Ranking wojewodztw'
        )
        fig_plotly.update_yaxes(autorange='reversed')
        fig_plotly.update_layout(height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = []
        if len(rank_cols) >= 2:
            latest_rank = rank_cols[-1]
            first_rank = rank_cols[0]
            ranking_pivot['zmiana_rank'] = ranking_pivot[first_rank] - ranking_pivot[latest_rank]

            gainers = ranking_pivot[ranking_pivot['zmiana_rank'] > 0].sort_values('zmiana_rank', ascending=False)
            losers = ranking_pivot[ranking_pivot['zmiana_rank'] < 0].sort_values('zmiana_rank')

            if not gainers.empty:
                for idx in gainers.head(3).index:
                    insights.append(f"Awans: {idx} (+{int(gainers.loc[idx, 'zmiana_rank'])} pozycji)")
            if not losers.empty:
                for idx in losers.head(3).index:
                    insights.append(f"Spadek: {idx} ({int(losers.loc[idx, 'zmiana_rank'])} pozycji)")

        return AnalysisResult(
            name="Ranking wojewodztw",
            description="Zmiana pozycji wojewodztw w rankingu kosztow w czasie",
            data=ranking_pivot.reset_index(),
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_correlations(self) -> AnalysisResult:
        df = self._get_data()

        pivot = df[df['poziom'] == 'WOJEWODZTWO'].pivot_table(
            index=['jednostka', 'rok'],
            columns='typ_kosztu',
            values='wartosc',
            aggfunc='sum'
        ).reset_index()

        typ_cols = [c for c in pivot.columns if c.startswith('ZASOBY')]
        corr_matrix = pivot[typ_cols].corr()

        corr_renamed = corr_matrix.rename(index=self.TYP_LABELS, columns=self.TYP_LABELS)

        fig, axes = plt.subplots(1, 2, figsize=(18, 8))

        im = axes[0].imshow(corr_renamed, cmap='RdYlGn', aspect='auto', vmin=-1, vmax=1)
        axes[0].set_xticks(range(len(corr_renamed.columns)))
        axes[0].set_yticks(range(len(corr_renamed.index)))
        axes[0].set_xticklabels(corr_renamed.columns, rotation=45, ha='right', fontsize=9)
        axes[0].set_yticklabels(corr_renamed.index, fontsize=9)
        axes[0].set_title('Macierz korelacji miedzy typami wlascicieli')

        for i in range(len(corr_renamed)):
            for j in range(len(corr_renamed.columns)):
                axes[0].text(j, i, f'{corr_renamed.iloc[i, j]:.2f}',
                             ha='center', va='center', fontsize=9,
                             color='white' if abs(corr_renamed.iloc[i, j]) > 0.5 else 'black')

        plt.colorbar(im, ax=axes[0])

        for typ in typ_cols:
            typ_data = pivot.groupby('rok')[typ].sum()
            label = self.TYP_LABELS.get(typ, typ)
            color = self.COLORS_7.get(typ, '#95a5a6')
            axes[1].plot(typ_data.index.astype(int), typ_data.values / 1000,
                         marker='o', linewidth=2, label=label, color=color)

        axes[1].set_xlabel('Rok')
        axes[1].set_ylabel('Koszty (mln zl)')
        axes[1].set_title('Porownanie trendow typow wlascicieli')
        axes[1].legend(fontsize=8)
        axes[1].grid(True, alpha=0.3)
        axes[1].xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        plt.tight_layout()
        static_path = self.output_dir / 'korelacje.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.imshow(
            corr_renamed,
            text_auto='.2f',
            title='Macierz korelacji',
            color_continuous_scale='RdYlGn',
            zmin=-1, zmax=1
        )
        fig_plotly.update_layout(height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = []
        typ_labels = list(corr_renamed.columns)
        for i in range(len(typ_labels)):
            for j in range(i + 1, len(typ_labels)):
                corr = corr_renamed.iloc[i, j]
                if abs(corr) > 0.7:
                    strength = "silna" if abs(corr) > 0.85 else "umiarkowana"
                    direction = "dodatnia" if corr > 0 else "ujemna"
                    insights.append(
                        f"{typ_labels[i]} vs {typ_labels[j]}: {strength} korelacja {direction} ({corr:.2f})")

        if not insights:
            insights.append("Brak silnych korelacji miedzy typami (|r| > 0.7)")

        return AnalysisResult(
            name="Analiza korelacji",
            description="Badanie zaleznosci miedzy wszystkimi typami wlascicieli",
            data=corr_renamed.reset_index(),
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_volatility(self) -> AnalysisResult:
        df = self._get_data()

        yearly = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'rok'])['wartosc'].sum().reset_index()

        stats = yearly.groupby('jednostka')['wartosc'].agg(['mean', 'std', 'min', 'max']).reset_index()
        stats['cv'] = (stats['std'] / stats['mean'] * 100).round(2)
        stats['range'] = stats['max'] - stats['min']
        stats = stats.sort_values('cv', ascending=False)

        fig, axes = plt.subplots(1, 2, figsize=(18, 8))

        n_stats = len(stats)
        colors_arr = plt.cm.RdYlGn_r(np.linspace(0, 1, n_stats)) if n_stats > 0 else ['gray']
        bars = axes[0].barh(stats['jednostka'], stats['cv'], color=colors_arr)
        axes[0].set_xlabel('Wspolczynnik zmiennosci CV (%)')
        axes[0].set_title('Zmiennosc kosztow wg wojewodztw')
        axes[0].axvline(x=stats['cv'].mean(), color='red', linestyle='--',
                        label=f'Srednia: {stats["cv"].mean():.1f}%')
        axes[0].legend()

        for bar, (_, row) in zip(bars, stats.iterrows()):
            axes[0].text(row['cv'] + 0.3, bar.get_y() + bar.get_height() / 2,
                         f"{row['cv']:.1f}%", va='center', fontsize=9)

        axes[1].errorbar(range(len(stats)), stats['mean'] / 1000,
                         yerr=stats['std'] / 1000, fmt='o', capsize=5, capthick=2,
                         markersize=8, color='steelblue')
        axes[1].set_xticks(range(len(stats)))
        axes[1].set_xticklabels(stats['jednostka'], rotation=45, ha='right')
        axes[1].set_ylabel('Koszty (mln zl)')
        axes[1].set_title('Srednia +/- odchylenie standardowe')

        plt.tight_layout()
        static_path = self.output_dir / 'zmiennosc.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.bar(
            stats,
            x='jednostka',
            y='cv',
            color='cv',
            color_continuous_scale='RdYlGn_r',
            title='Wspolczynnik zmiennosci kosztow wg wojewodztw'
        )
        fig_plotly.update_layout(height=500)
        interactive_html = fig_plotly.to_html(full_html=False)

        most_stable = stats.nsmallest(3, 'cv')['jednostka'].tolist()
        most_volatile = stats.nlargest(3, 'cv')['jednostka'].tolist()

        insights = [
            f"Najbardziej stabilne: {', '.join(most_stable)}",
            f"Najbardziej zmienne: {', '.join(most_volatile)}",
            f"Sredni CV: {stats['cv'].mean():.1f}%",
            f"Mediana CV: {stats['cv'].median():.1f}%"
        ]

        return AnalysisResult(
            name="Analiza zmiennosci",
            description="Badanie stabilnosci kosztow w czasie (wspolczynnik zmiennosci CV)",
            data=stats,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_owner_comparison(self) -> AnalysisResult:
        df = self._get_data()

        typ_summary = df[df['poziom'] == 'WOJEWODZTWO'].groupby('typ_kosztu').agg({
            'wartosc': ['sum', 'mean', 'std', 'count']
        }).reset_index()
        typ_summary.columns = ['typ_kosztu', 'suma', 'srednia', 'std', 'count']
        typ_summary['typ_label'] = typ_summary['typ_kosztu'].map(self.TYP_LABELS)
        typ_summary['udzial'] = (typ_summary['suma'] / typ_summary['suma'].sum() * 100).round(1)
        typ_summary['cv'] = (typ_summary['std'] / typ_summary['srednia'] * 100).round(1)
        typ_summary = typ_summary.sort_values('suma', ascending=False)

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        colors_pie = [self.COLORS_7.get(t, '#95a5a6') for t in typ_summary['typ_kosztu']]
        wedges, texts, autotexts = axes[0, 0].pie(
            typ_summary['suma'],
            labels=typ_summary['typ_label'],
            autopct='%1.1f%%',
            colors=colors_pie,
            explode=[0.03] * len(typ_summary)
        )
        axes[0, 0].set_title('Udzial typow w calkowitych kosztach')

        bars = axes[0, 1].barh(typ_summary['typ_label'], typ_summary['suma'] / 1000,
                               color=colors_pie)
        axes[0, 1].set_xlabel('Suma kosztow (mln zl)')
        axes[0, 1].set_title('Laczne koszty wg typu wlasciciela')

        for bar, val in zip(bars, typ_summary['suma'] / 1000):
            axes[0, 1].text(val + 0.5, bar.get_y() + bar.get_height() / 2,
                            f'{val:.1f}', va='center', fontsize=9)

        yearly = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['rok', 'typ_kosztu'])['wartosc'].sum().reset_index()
        yearly['typ_label'] = yearly['typ_kosztu'].map(self.TYP_LABELS)
        yearly_pivot = yearly.pivot(index='rok', columns='typ_label', values='wartosc')
        yearly_pct = yearly_pivot.div(yearly_pivot.sum(axis=1), axis=0) * 100

        yearly_pct.plot(kind='area', stacked=True, ax=axes[1, 0], alpha=0.8)
        axes[1, 0].set_xlabel('Rok')
        axes[1, 0].set_ylabel('Udzial (%)')
        axes[1, 0].set_title('Ewolucja struktury typow w czasie')
        axes[1, 0].legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=8)
        axes[1, 0].set_ylim(0, 100)

        axes[1, 1].bar(typ_summary['typ_label'], typ_summary['cv'], color=colors_pie)
        axes[1, 1].set_ylabel('Wspolczynnik zmiennosci CV (%)')
        axes[1, 1].set_title('Zmiennosc kosztow wg typu wlasciciela')
        axes[1, 1].tick_params(axis='x', rotation=45)
        axes[1, 1].axhline(y=typ_summary['cv'].mean(), color='red', linestyle='--',
                           label=f'Srednia: {typ_summary["cv"].mean():.1f}%')
        axes[1, 1].legend()

        plt.tight_layout()
        static_path = self.output_dir / 'porownanie_wlascicieli.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.treemap(
            df[df['poziom'] == 'WOJEWODZTWO'],
            path=['typ_label', 'jednostka'],
            values='wartosc',
            title='Struktura kosztow - treemap',
            color='typ_label'
        )
        fig_plotly.update_layout(height=700)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = ["Ranking typow wlascicieli wg sumy kosztow:"]
        for i, (_, row) in enumerate(typ_summary.iterrows(), 1):
            insights.append(f"{i}. {row['typ_label']}: {row['udzial']:.1f}% ({row['suma'] / 1000:.1f} mln zl)")

        return AnalysisResult(
            name="Porownanie typow wlascicieli",
            description="Szczegolowa analiza kosztow wg wszystkich typow wlascicieli zasobow",
            data=typ_summary,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_regional_heatmap(self) -> AnalysisResult:
        df = self._get_data()

        pivot = df[df['poziom'] == 'WOJEWODZTWO'].pivot_table(
            index='jednostka',
            columns='typ_kosztu',
            values='wartosc',
            aggfunc='sum'
        ).fillna(0)

        pivot_renamed = pivot.rename(columns=self.TYP_LABELS)
        pivot_normalized = pivot_renamed.div(pivot_renamed.sum(axis=1), axis=0) * 100

        fig, axes = plt.subplots(1, 2, figsize=(20, 10))

        im1 = axes[0].imshow(pivot_renamed.values / 1000, cmap='YlOrRd', aspect='auto')
        axes[0].set_xticks(range(len(pivot_renamed.columns)))
        axes[0].set_yticks(range(len(pivot_renamed.index)))
        axes[0].set_xticklabels(pivot_renamed.columns, rotation=45, ha='right', fontsize=9)
        axes[0].set_yticklabels(pivot_renamed.index, fontsize=9)
        axes[0].set_title('Heatmapa kosztow (mln zl)')
        plt.colorbar(im1, ax=axes[0], label='mln zl')

        im2 = axes[1].imshow(pivot_normalized.values, cmap='YlOrRd', aspect='auto')
        axes[1].set_xticks(range(len(pivot_normalized.columns)))
        axes[1].set_yticks(range(len(pivot_normalized.index)))
        axes[1].set_xticklabels(pivot_normalized.columns, rotation=45, ha='right', fontsize=9)
        axes[1].set_yticklabels(pivot_normalized.index, fontsize=9)
        axes[1].set_title('Heatmapa struktury (%)')
        plt.colorbar(im2, ax=axes[1], label='%')

        plt.tight_layout()
        static_path = self.output_dir / 'heatmapa_regionalna.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.imshow(
            pivot_normalized,
            title='Struktura kosztow wg wojewodztw i typow',
            color_continuous_scale='YlOrRd',
            labels={'color': 'Udzial (%)'}
        )
        fig_plotly.update_layout(height=700)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = ["Dominujacy typ wg wojewodztwa:"]
        for woj in pivot_normalized.index:
            dominant = pivot_normalized.loc[woj].idxmax()
            pct = pivot_normalized.loc[woj, dominant]
            insights.append(f"  {woj}: {dominant} ({pct:.1f}%)")

        return AnalysisResult(
            name="Heatmapa regionalna",
            description="Wizualizacja struktury kosztow wg wojewodztw i typow wlascicieli",
            data=pivot_normalized.reset_index(),
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights[:10]
        )

    def analyze_top_changes(self) -> AnalysisResult:
        df = self._get_data()

        changes = df[df['poziom'] == 'WOJEWODZTWO'].groupby(['jednostka', 'typ_kosztu', 'rok'])[
            'wartosc'].sum().reset_index()
        changes = changes.sort_values(['jednostka', 'typ_kosztu', 'rok'])

        changes['prev_val'] = changes.groupby(['jednostka', 'typ_kosztu'])['wartosc'].shift(1)
        changes['zmiana_pct'] = ((changes['wartosc'] - changes['prev_val']) / changes['prev_val'] * 100).round(1)
        changes['zmiana_abs'] = changes['wartosc'] - changes['prev_val']
        changes['typ_label'] = changes['typ_kosztu'].map(self.TYP_LABELS)

        changes_clean = changes.dropna(subset=['zmiana_pct'])

        top_increases = changes_clean.nlargest(15, 'zmiana_pct')
        top_decreases = changes_clean.nsmallest(15, 'zmiana_pct')

        fig, axes = plt.subplots(1, 2, figsize=(18, 10))

        labels_inc = [f"{r['jednostka'][:10]}\n{r['typ_label'][:8]}\n({int(r['rok'])})"
                      for _, r in top_increases.iterrows()]
        colors_inc = [self.COLORS_7.get(t, '#95a5a6') for t in top_increases['typ_kosztu']]
        axes[0].barh(range(len(top_increases)), top_increases['zmiana_pct'], color=colors_inc)
        axes[0].set_yticks(range(len(top_increases)))
        axes[0].set_yticklabels(labels_inc, fontsize=8)
        axes[0].set_xlabel('Zmiana (%)')
        axes[0].set_title('TOP 15 najwiekszych wzrostow')
        axes[0].invert_yaxis()

        labels_dec = [f"{r['jednostka'][:10]}\n{r['typ_label'][:8]}\n({int(r['rok'])})"
                      for _, r in top_decreases.iterrows()]
        colors_dec = [self.COLORS_7.get(t, '#95a5a6') for t in top_decreases['typ_kosztu']]
        axes[1].barh(range(len(top_decreases)), top_decreases['zmiana_pct'], color=colors_dec)
        axes[1].set_yticks(range(len(top_decreases)))
        axes[1].set_yticklabels(labels_dec, fontsize=8)
        axes[1].set_xlabel('Zmiana (%)')
        axes[1].set_title('TOP 15 najwiekszych spadkow')
        axes[1].invert_yaxis()

        plt.tight_layout()
        static_path = self.output_dir / 'top_zmiany.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = px.bar(
            pd.concat([top_increases.head(10), top_decreases.head(10)]),
            x='zmiana_pct',
            y='jednostka',
            color='typ_label',
            orientation='h',
            title='TOP wzrosty i spadki kosztow',
            hover_data=['rok', 'zmiana_abs']
        )
        fig_plotly.update_layout(height=600)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = ["Najwieksze wzrosty:"]
        for _, row in top_increases.head(5).iterrows():
            insights.append(f"  +{row['zmiana_pct']:.0f}%: {row['jednostka']} - {row['typ_label']} ({int(row['rok'])})")

        insights.append("Najwieksze spadki:")
        for _, row in top_decreases.head(5).iterrows():
            insights.append(f"  {row['zmiana_pct']:.0f}%: {row['jednostka']} - {row['typ_label']} ({int(row['rok'])})")

        return AnalysisResult(
            name="Top wzrosty i spadki",
            description="Ranking najwiekszych zmian kosztow rok do roku",
            data=pd.concat([top_increases, top_decreases]),
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def analyze_summary_statistics(self) -> AnalysisResult:
        df = self._get_data()
        df_woj = df[df['poziom'] == 'WOJEWODZTWO']

        summary = {
            'Liczba rekordow': len(df_woj),
            'Liczba wojewodztw': df_woj['jednostka'].nunique(),
            'Liczba lat': df_woj['rok'].nunique(),
            'Zakres lat': f"{int(df_woj['rok'].min())} - {int(df_woj['rok'].max())}",
            'Liczba typow wlascicieli': df_woj['typ_kosztu'].nunique(),
            'Suma kosztow (mln zl)': round(df_woj['wartosc'].sum() / 1000, 2),
            'Srednia wartosc (tys. zl)': round(df_woj['wartosc'].mean(), 2),
            'Mediana (tys. zl)': round(df_woj['wartosc'].median(), 2),
            'Odchylenie std (tys. zl)': round(df_woj['wartosc'].std(), 2),
            'Min (tys. zl)': round(df_woj['wartosc'].min(), 2),
            'Max (tys. zl)': round(df_woj['wartosc'].max(), 2),
            'Wspolczynnik zmiennosci (%)': round(df_woj['wartosc'].std() / df_woj['wartosc'].mean() * 100, 2)
        }

        summary_df = pd.DataFrame([summary]).T.reset_index()
        summary_df.columns = ['Metryka', 'Wartosc']

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        axes[0, 0].hist(df_woj['wartosc'], bins=40, color='steelblue', edgecolor='white', alpha=0.7)
        axes[0, 0].axvline(df_woj['wartosc'].mean(), color='red', linestyle='--', linewidth=2, label='Srednia')
        axes[0, 0].axvline(df_woj['wartosc'].median(), color='green', linestyle='--', linewidth=2, label='Mediana')
        axes[0, 0].set_xlabel('Wartosc (tys. zl)')
        axes[0, 0].set_ylabel('Czestotliwosc')
        axes[0, 0].set_title('Rozklad wartosci kosztow')
        axes[0, 0].legend()

        typy = sorted(df_woj['typ_kosztu'].unique())
        bp = axes[0, 1].boxplot([df_woj[df_woj['typ_kosztu'] == t]['wartosc'] for t in typy],
                                labels=[self.TYP_LABELS.get(t, t)[:10] for t in typy],
                                patch_artist=True)
        colors_bp = [self.COLORS_7.get(t, '#95a5a6') for t in typy]
        for patch, color in zip(bp['boxes'], colors_bp):
            patch.set_facecolor(color)
        axes[0, 1].set_ylabel('Wartosc (tys. zl)')
        axes[0, 1].set_title('Rozklad wartosci wg typow wlascicieli')
        axes[0, 1].tick_params(axis='x', rotation=45)

        yearly_sum = df_woj.groupby('rok')['wartosc'].sum() / 1000
        yearly_count = df_woj.groupby('rok').size()

        ax2 = axes[1, 0].twinx()
        axes[1, 0].bar(yearly_sum.index.astype(int), yearly_sum.values,
                       color='steelblue', alpha=0.7, label='Suma kosztow')
        ax2.plot(yearly_count.index.astype(int), yearly_count.values,
                 color='red', marker='o', linewidth=2, label='Liczba rekordow')
        axes[1, 0].set_xlabel('Rok')
        axes[1, 0].set_ylabel('Suma kosztow (mln zl)', color='steelblue')
        ax2.set_ylabel('Liczba rekordow', color='red')
        axes[1, 0].set_title('Suma kosztow i liczba rekordow w czasie')
        axes[1, 0].xaxis.set_major_locator(plt.MaxNLocator(integer=True))

        cell_text = [[row['Metryka'], str(row['Wartosc'])] for _, row in summary_df.iterrows()]
        table = axes[1, 1].table(
            cellText=cell_text,
            colLabels=['Metryka', 'Wartosc'],
            loc='center',
            cellLoc='left'
        )
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1.2, 2.0)
        axes[1, 1].axis('off')
        axes[1, 1].set_title('Podsumowanie statystyczne', fontsize=14, fontweight='bold')

        plt.tight_layout()
        static_path = self.output_dir / 'podsumowanie.png'
        plt.savefig(static_path, dpi=150, bbox_inches='tight')
        plt.close()

        fig_plotly = go.Figure()
        for typ in sorted(df_woj['typ_kosztu'].unique()):
            fig_plotly.add_trace(go.Box(
                y=df_woj[df_woj['typ_kosztu'] == typ]['wartosc'],
                name=self.TYP_LABELS.get(typ, typ),
                marker_color=self.COLORS_7.get(typ, '#95a5a6')
            ))
        fig_plotly.update_layout(title='Rozklad wartosci', height=500)
        interactive_html = fig_plotly.to_html(full_html=False)

        insights = [f"{k}: {v}" for k, v in summary.items()]

        return AnalysisResult(
            name="Podsumowanie statystyczne",
            description="xyz",
            data=summary_df,
            chart_static=static_path,
            chart_interactive=interactive_html,
            insights=insights
        )

    def get_summary_stats(self) -> Dict:
        df = self._get_data()

        return {
            'total_records': len(df),
            'years': sorted([int(y) for y in df['rok'].unique().tolist()]),
            'regions_count': df[df['poziom'] == 'WOJEWODZTWO']['jednostka'].nunique(),
            'types_count': df['typ_kosztu'].nunique(),
            'categories': [self.TYP_LABELS.get(t, t) for t in df['typ_kosztu'].unique()],
            'total_value': df[df['poziom'] == 'WOJEWODZTWO']['wartosc'].sum(),
            'avg_value': df[df['poziom'] == 'WOJEWODZTWO']['wartosc'].mean(),
            'min_value': df['wartosc'].min(),
            'max_value': df['wartosc'].max()
        }