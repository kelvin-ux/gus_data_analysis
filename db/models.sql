DROP SCHEMA IF EXISTS gus CASCADE;
CREATE SCHEMA gus;
SET search_path TO gus;

CREATE TABLE dim_jednostka (
    id              SERIAL PRIMARY KEY,
    kod_gus         VARCHAR(10) NOT NULL UNIQUE,
    nazwa           VARCHAR(200) NOT NULL,
    poziom          VARCHAR(20) NOT NULL CHECK (poziom IN ('POLSKA', 'WOJEWODZTWO', 'POWIAT')),
    kod_wojewodztwa VARCHAR(10),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_typ_kosztu (
    id              SERIAL PRIMARY KEY,
    kod             VARCHAR(50) NOT NULL UNIQUE,
    nazwa           VARCHAR(300) NOT NULL,
    kategoria       VARCHAR(50) NOT NULL CHECK (kategoria IN ('PUBLICZNE', 'SPOLDZIELCZE', 'SPOLECZNE', 'PRYWATNE')),
    opis            TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_okres (
    id              SERIAL PRIMARY KEY,
    rok             INTEGER NOT NULL UNIQUE CHECK (rok >= 2000 AND rok <= 2100),
    data_publikacji TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE log_import (
    id                  SERIAL PRIMARY KEY,
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    status              VARCHAR(20) NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    source_file         VARCHAR(500),
    source_hash         VARCHAR(64),
    rows_processed      INTEGER DEFAULT 0,
    rows_inserted       INTEGER DEFAULT 0,
    rows_updated        INTEGER DEFAULT 0,
    rows_failed         INTEGER DEFAULT 0,
    error_message       TEXT,
    created_by          VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    record_id       INTEGER NOT NULL,
    operation       VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    old_data        JSONB,
    new_data        JSONB,
    changed_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by      VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE TABLE validation_errors (
    id              BIGSERIAL PRIMARY KEY,
    import_id       INTEGER REFERENCES log_import(id),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_data     JSONB NOT NULL,
    error_type      VARCHAR(50) NOT NULL,
    error_field     VARCHAR(100),
    error_message   TEXT NOT NULL,
    raw_value       TEXT
);

CREATE INDEX idx_validation_import ON validation_errors(import_id);
CREATE INDEX idx_validation_type ON validation_errors(error_type);
CREATE INDEX idx_validation_time ON validation_errors(created_at DESC);

CREATE TABLE data_quality_report (
    id                  SERIAL PRIMARY KEY,
    import_id           INTEGER REFERENCES log_import(id),
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_rows          INTEGER NOT NULL,
    null_count          INTEGER NOT NULL DEFAULT 0,
    null_percentage     NUMERIC(5,2),
    duplicate_count     INTEGER NOT NULL DEFAULT 0,
    outlier_count       INTEGER NOT NULL DEFAULT 0,
    negative_count      INTEGER NOT NULL DEFAULT 0,
    validation_passed   BOOLEAN NOT NULL DEFAULT FALSE,
    issues              JSONB,
    min_value           NUMERIC(15,2),
    max_value           NUMERIC(15,2),
    avg_value           NUMERIC(15,2),
    median_value        NUMERIC(15,2),
    stddev_value        NUMERIC(15,2)
);

CREATE TABLE fact_koszty (
    id              BIGSERIAL PRIMARY KEY,
    jednostka_id    INTEGER NOT NULL REFERENCES dim_jednostka(id),
    typ_kosztu_id   INTEGER NOT NULL REFERENCES dim_typ_kosztu(id),
    okres_id        INTEGER NOT NULL REFERENCES dim_okres(id),
    wartosc         NUMERIC(15,2),
    import_id       INTEGER REFERENCES log_import(id),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_koszty_natural_key
        UNIQUE (jednostka_id, typ_kosztu_id, okres_id)
);

CREATE INDEX idx_jednostka_poziom ON dim_jednostka(poziom);
CREATE INDEX idx_jednostka_wojewodztwo ON dim_jednostka(kod_wojewodztwa);
CREATE INDEX idx_typ_kosztu_kategoria ON dim_typ_kosztu(kategoria);
CREATE INDEX idx_fact_jednostka ON fact_koszty(jednostka_id);
CREATE INDEX idx_fact_typ_kosztu ON fact_koszty(typ_kosztu_id);
CREATE INDEX idx_fact_okres ON fact_koszty(okres_id);
CREATE INDEX idx_fact_import ON fact_koszty(import_id);
CREATE INDEX idx_fact_analiza ON fact_koszty(okres_id, jednostka_id, typ_kosztu_id);
CREATE INDEX idx_audit_table ON audit_log(table_name);
CREATE INDEX idx_audit_record ON audit_log(table_name, record_id);
CREATE INDEX idx_audit_time ON audit_log(changed_at DESC);
CREATE INDEX idx_import_status ON log_import(status);
CREATE INDEX idx_import_time ON log_import(started_at DESC);

CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO gus.audit_log (table_name, record_id, operation, new_data)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD IS DISTINCT FROM NEW THEN
            INSERT INTO gus.audit_log (table_name, record_id, operation, old_data, new_data)
            VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
            NEW.updated_at = CURRENT_TIMESTAMP;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO gus.audit_log (table_name, record_id, operation, old_data)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_fact_koszty_audit
    AFTER INSERT OR UPDATE OR DELETE ON fact_koszty
    FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

CREATE OR REPLACE FUNCTION fn_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jednostka_updated
    BEFORE UPDATE ON dim_jednostka
    FOR EACH ROW EXECUTE FUNCTION fn_update_timestamp();

CREATE OR REPLACE VIEW v_koszty_pelne AS
SELECT
    f.id,
    j.kod_gus,
    j.nazwa AS jednostka_nazwa,
    j.poziom,
    j.kod_wojewodztwa,
    t.kod AS typ_kosztu_kod,
    t.nazwa AS typ_kosztu_nazwa,
    t.kategoria,
    o.rok,
    f.wartosc,
    f.wartosc * 1000 AS wartosc_zl,
    f.created_at,
    f.updated_at
FROM fact_koszty f
JOIN dim_jednostka j ON f.jednostka_id = j.id
JOIN dim_typ_kosztu t ON f.typ_kosztu_id = t.id
JOIN dim_okres o ON f.okres_id = o.id;

CREATE OR REPLACE VIEW v_koszty_wojewodztwa AS
SELECT
    j.kod_gus,
    j.nazwa AS wojewodztwo,
    o.rok,
    t.kategoria,
    SUM(f.wartosc) AS suma_kosztow,
    AVG(f.wartosc) AS srednia_kosztow,
    COUNT(DISTINCT CASE WHEN j2.poziom = 'POWIAT' THEN j2.id END) AS liczba_powiatow
FROM fact_koszty f
JOIN dim_jednostka j ON f.jednostka_id = j.id AND j.poziom = 'WOJEWODZTWO'
JOIN dim_typ_kosztu t ON f.typ_kosztu_id = t.id
JOIN dim_okres o ON f.okres_id = o.id
LEFT JOIN dim_jednostka j2 ON j2.kod_wojewodztwa = j.kod_gus
GROUP BY j.kod_gus, j.nazwa, o.rok, t.kategoria;

CREATE OR REPLACE VIEW v_trend_roczny AS
SELECT
    o.rok,
    j.poziom,
    t.kod AS typ_kosztu,
    t.kategoria,
    SUM(f.wartosc) AS suma_kosztow,
    AVG(f.wartosc) AS srednia_kosztow,
    COUNT(*) AS liczba_rekordow,
    LAG(SUM(f.wartosc)) OVER (PARTITION BY j.poziom, t.kod ORDER BY o.rok) AS poprzedni_rok,
    ROUND(
        (SUM(f.wartosc) - LAG(SUM(f.wartosc)) OVER (PARTITION BY j.poziom, t.kod ORDER BY o.rok))
        / NULLIF(LAG(SUM(f.wartosc)) OVER (PARTITION BY j.poziom, t.kod ORDER BY o.rok), 0) * 100
    , 2) AS zmiana_procent
FROM fact_koszty f
JOIN dim_jednostka j ON f.jednostka_id = j.id
JOIN dim_typ_kosztu t ON f.typ_kosztu_id = t.id
JOIN dim_okres o ON f.okres_id = o.id
GROUP BY o.rok, j.poziom, t.kod, t.kategoria
ORDER BY j.poziom, t.kod, o.rok;

CREATE OR REPLACE VIEW v_struktura_kosztow AS
WITH totals AS (
    SELECT
        o.rok,
        SUM(f.wartosc) AS total
    FROM fact_koszty f
    JOIN dim_jednostka j ON f.jednostka_id = j.id
    JOIN dim_typ_kosztu t ON f.typ_kosztu_id = t.id
    JOIN dim_okres o ON f.okres_id = o.id
    GROUP BY o.rok
)
SELECT
    o.rok,
    t.kod,
    t.nazwa,
    t.kategoria,
    SUM(f.wartosc) AS wartosc,
    ROUND(SUM(f.wartosc) / NULLIF(tot.total, 0) * 100, 2) AS udzial_procent
FROM fact_koszty f
JOIN dim_jednostka j ON f.jednostka_id = j.id
JOIN dim_typ_kosztu t ON f.typ_kosztu_id = t.id
JOIN dim_okres o ON f.okres_id = o.id
JOIN totals tot ON tot.rok = o.rok
GROUP BY o.rok, t.kod, t.nazwa, t.kategoria, tot.total
ORDER BY o.rok, t.kategoria, wartosc DESC;

CREATE OR REPLACE VIEW v_top_zmiany AS
WITH koszty_pivot AS (
    SELECT
        j.id,
        j.kod_gus,
        j.nazwa,
        j.poziom,
        t.kod AS typ_kosztu,
        MAX(CASE WHEN o.rok = 2022 THEN f.wartosc END) AS rok_2022,
        MAX(CASE WHEN o.rok = 2024 THEN f.wartosc END) AS rok_2024
    FROM fact_koszty f
    JOIN dim_jednostka j ON f.jednostka_id = j.id
    JOIN dim_typ_kosztu t ON f.typ_kosztu_id = t.id
    JOIN dim_okres o ON f.okres_id = o.id
    WHERE j.poziom = 'WOJEWODZTWO'
    GROUP BY j.id, j.kod_gus, j.nazwa, j.poziom, t.kod
)
SELECT
    kod_gus,
    nazwa,
    poziom,
    typ_kosztu,
    rok_2022,
    rok_2024,
    rok_2024 - rok_2022 AS zmiana_abs,
    ROUND((rok_2024 - rok_2022) / NULLIF(rok_2022, 0) * 100, 2) AS zmiana_procent
FROM koszty_pivot
WHERE rok_2022 > 0 AND rok_2024 > 0
ORDER BY zmiana_procent DESC;

CREATE OR REPLACE VIEW v_ostatni_import AS
SELECT
    li.*,
    dqr.total_rows,
    dqr.null_percentage,
    dqr.validation_passed,
    dqr.issues
FROM log_import li
LEFT JOIN data_quality_report dqr ON dqr.import_id = li.id
ORDER BY li.started_at DESC
LIMIT 1;