"""
Update accredagency in Institutions table and
Load data for yearly Students, Financials, and Academics tables

Usage:
    python load-scorecard.py ../data/scorecard/scorecard_2019.csv
    python load-scorecard.py ../data/scorecard/scorecard_2020.csv
    python load-scorecard.py ../data/scorecard/scorecard_2021.csv
    python load-scorecard.py ../data/scorecard/scorecard_2022.csv

Note:
    - Rename csv files to end with 4-digit year, e.g., scorecard_2022.csv
    - Run the script for each year file by chronological order (2019-2022)
"""

import sys
import pandas as pd
import psycopg


def clean(value):
    """
    Convert -999, blanks, and NULL to None
    Input:
        value: any
    Output:
        cleaned value or None
    """
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
    """
    Extract 4-digit year from filename
    Input:
        path: str, path to the CSV file
    Output:
        year: int, 4-digit year
    """
    filename = path.split("/")[-1]
    filename = filename.split(".")[0]
    parts = filename.split("_")

    for part in reversed(parts):
        if part.isdigit():
            return int(part)


def build_students_rows(df, year):
    """
    Build rows for Students table
    Input:
        df: pandas.DataFrame, the data to process
        year: int, 4-digit year
    Output:
        rows: list of tuples, each tuple is a row for Students table
    """
    rows = []
    for _, rec in df.iterrows():
        rows.append((
            clean(int(rec.get("UNITID"))),
            year,
            clean(rec.get("ADM_RATE")),
            clean(rec.get("UGDS")),
            clean(rec.get("ACTCMMID")),
            clean(rec.get("CDR2")),
            clean(rec.get("CDR3"))
        ))
    return rows


def build_financials_rows(df, year):
    """
    Build rows for Financials table
    Input:
        df: pandas.DataFrame, the data to process
        year: int, 4-digit year
    Output:
        rows: list of tuples, each tuple is a row for Financials table
    """
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
    """
    Build rows for Academics table
    Input:
        df: pandas.DataFrame, the data to process
        year: int, 4-digit year
    Output:
        rows: list of tuples, each tuple is a row for Academics table
    """
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
    '''
    Updates accredagency in Institutions table and
    inserts data into Students, Financials, and Academics tables.
    '''
    # Load the CSV file
    csv_path = sys.argv[1]
    year = extract_year(csv_path)

    data = pd.read_csv(csv_path, low_memory=False, encoding="latin1")
    data.columns = [c.strip().upper() for c in data.columns]
    print(f"Loaded {len(data)} rows for year {year}.")

    # Build data for all 3 tables
    students_rows = build_students_rows(data, year)
    financials_rows = build_financials_rows(data, year)
    academics_rows = build_academics_rows(data, year)

    # SQL statements
    accredagency_sql = ("UPDATE Institutions SET accredagency = %s "
                        "WHERE institution_id = %s")
    students_sql = ("INSERT INTO Students (institution_id, year, adm_rate, "
                    "num_students, act, cdr2, cdr3) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)")
    financials_sql = ("INSERT INTO Financials (institution_id, year, "
                      "tuitionfee_in, tuitionfee_out, tuitionfee_prog, "
                      "tuitfte, avgfacsal) VALUES (%s,%s,%s,%s,%s,%s,%s)")
    academics_sql = ("INSERT INTO Academics (institution_id, year, preddeg, "
                     "highdeg, stufacr) VALUES (%s,%s,%s,%s,%s)")
    check_sql = "SELECT 1 FROM Institutions WHERE institution_id = %s;"

    # Connect to the database
    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname="",
        user="",
        password=""
    )
    cursor = conn.cursor()

    updated = 0
    print("Updating accredagency in Institutions...")
    try:
        # Update rows for accredagency
        for i, row in data.iterrows():
            accredagency = clean(row["ACCREDAGENCY"])
            unitid = row["UNITID"]
            try:
                cursor.execute(accredagency_sql, (accredagency, unitid))
                if cursor.rowcount > 0:
                    updated += cursor.rowcount
                if (i + 1) % 500 == 0:
                    print(f"Updated {i + 1} records of accredagency in "
                          f"Institutions...")
            except Exception as e:
                print(f"[ERROR] Row {i+1} (UNITID={unitid}) failed: {e}")
                raise
        conn.commit()
        print(f"Done. {updated} records of accredagency in Institutions "
              f"updated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Update of accredagency in Institutions failed: {e}")

    inserted = {"Students": 0, "Financials": 0, "Academics": 0}
    # Insert rows
    print("Inserting into Students...")
    for i, row in enumerate(students_rows, start=1):
        cursor.execute(check_sql, (row[0],))
        if cursor.fetchone() is None:
            continue
        try:
            cursor.execute(students_sql, row)
            inserted["Students"] += 1
            if i % 500 == 0:
                print(f"Students: {i} rows inserted...")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Students row {i} failed: {e}")
            sys.exit(1)
    conn.commit()
    print(f"Done. Inserted {inserted['Students']} records into Students")

    print("Inserting into Financials...")
    for i, row in enumerate(financials_rows, start=1):
        cursor.execute(check_sql, (row[0],))
        if cursor.fetchone() is None:
            continue
        try:
            cursor.execute(financials_sql, row)
            inserted["Financials"] += 1
            if i % 500 == 0:
                print(f"Financials: {i} rows inserted...")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Financials row {i} failed: {e}")
            sys.exit(1)
    conn.commit()
    print(f"Done. Inserted {inserted['Financials']} records into Financials")

    print("Inserting into Academics...")
    for i, row in enumerate(academics_rows, start=1):
        cursor.execute(check_sql, (row[0],))
        if cursor.fetchone() is None:
            continue
        try:
            cursor.execute(academics_sql, row)
            inserted["Academics"] += 1
            if i % 500 == 0:
                print(f"Academics: {i} rows inserted...")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Academics row {i} failed: {e}")
            sys.exit(1)
    conn.commit()
    print(f"Done. Inserted {inserted['Academics']} records into Academics")
    cursor.close()
    conn.close()
    print(f"All insertions done for year {year}.")


if __name__ == "__main__":
    main()
