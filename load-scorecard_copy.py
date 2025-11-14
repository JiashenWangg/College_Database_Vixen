"""
Unified loader for yearly Students, Financials, and Academics tables.

Usage:
    python load-scorecard_copy.py ../data/scorecard/scorecard_test_2022.csv
"""

import sys
import pandas as pd
import psycopg


def clean(value):
    """Convert -999, blanks, and NULL to None."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip()
        if v in {"", "NA", "N/A", "nan", "NULL"}:
            return None
        if v in {"-3", "-2", "-999"}:
            return None
        return v
    return value


def extract_year(path):
    """Extract 4-digit year from filename"""
    filename = path.split("/")[-1]
    filename = filename.split(".")[0]
    parts = filename.split("_")

    for part in reversed(parts):
        if part.isdigit():
            return int(part)


def build_students_rows(df, year):
    """Build rows for Students table if columns exist."""
    rows = []
    for _, rec in df.iterrows():
        rows.append((
            clean(int(rec.get("UNITID"))),
            year,
            clean(rec.get("ADM_RATE")),
            clean(rec.get("UGDS")),
            clean(rec.get("ACTCMMID")),
            clean(rec.get("CDR2")),
            clean(rec.get("CDR3")),
            clean(rec.get("FIRSTGEN")),
            clean(rec.get("FAMINC")),
        ))
    return rows


def build_financials_rows(df, year):
    """Build rows for Financials table if columns exist."""
    rows = []
    for _, rec in df.iterrows():
        rows.append((
            clean(int(rec.get("UNITID"))),
            year,
            clean(rec.get("TUITIONFEE_IN")),
            clean(rec.get("TUITIONFEE_OUT")),
            clean(rec.get("TUITIONFEE_PROG")),
            clean(rec.get("TUITFTE")),
            clean(rec.get("AVGFACSAL")),
        ))
    return rows


def build_academics_rows(df, year):
    """Build rows for Academics table if columns exist."""
    rows = []
    for _, rec in df.iterrows():
        rows.append((
            clean(int(rec.get("UNITID"))),
            year,
            clean(rec.get("PREDDEG")),
            clean(rec.get("HIGHDEG")),
            clean(rec.get("STUFACR")),
        ))
    return rows


def main():
    if len(sys.argv) != 2:
        print("Usage: python load-scorecard_all.py path/to/MERGEDYYYY_PP.csv")
        sys.exit(2)

    csv_path = sys.argv[1]
    year = extract_year(csv_path)

    df = pd.read_csv(csv_path, low_memory=False, encoding="latin1")
    df.columns = [c.strip().upper() for c in df.columns]
    print(f"Loaded {len(df)} records for year {year}.")

    # Build data for all 3 tables
    students_rows = build_students_rows(df, year)
    financials_rows = build_financials_rows(df, year)
    academics_rows = build_academics_rows(df, year)

    # SQL
    students_sql = ("INSERT INTO Students (institution_id, year, adm_rate, "
                    "num_students, act, cdr2, cdr3, first_gen, "
                    "avg_family_income) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    financials_sql = ("INSERT INTO Financials (institution_id, year, "
                      "tuitionfee_in, tuitionfee_out, tuitionfee_prog, "
                      "tuitfte, avgfacsal) VALUES (%s,%s,%s,%s,%s,%s,%s)")
    academics_sql = ("INSERT INTO Academics (institution_id, year, preddeg, "
                     "highdeg, stufacr) VALUES (%s,%s,%s,%s,%s)")
    check_sql = "SELECT 1 FROM Institutions WHERE institution_id = %s;"

    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname="agehr",
        user="agehr",
        password="?eMc2GnHzV"
    )
    cursor = conn.cursor()
    inserted = {"Students": 0, "Financials": 0, "Academics": 0}

    for i, row in enumerate(students_rows, start=1):
        cursor.execute(check_sql, (row[0],))
        if cursor.fetchone() is None:
            continue
        try:
            cursor.execute(students_sql, row)
            inserted["Students"] += 1
            if i % 10 == 0:
                print(f"   Students: {i} rows processed...")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Students row {i} failed: {e}")
            sys.exit(1)
    for i, row in enumerate(financials_rows, start=1):
        cursor.execute(check_sql, (row[0],))
        if cursor.fetchone() is None:
            continue
        try:
            cursor.execute(financials_sql, row)
            inserted["Financials"] += 1
            if i % 10 == 0:
                print(f"   Financials: {i} rows processed...")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Financials row {i} failed: {e}")
            sys.exit(1)
    for i, row in enumerate(academics_rows, start=1):
        cursor.execute(check_sql, (row[0],))
        if cursor.fetchone() is None:
            continue
        try:
            cursor.execute(academics_sql, row)
            inserted["Academics"] += 1
            if i % 10 == 0:
                print(f"   Academics: {i} rows processed...")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Academics row {i} failed: {e}")
            sys.exit(1)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {inserted['Students']} Students rows.")
    print(f"Inserted {inserted['Financials']} Financials rows.")
    print(f"Inserted {inserted['Academics']} Academics rows.")


if __name__ == "__main__":
    main()
