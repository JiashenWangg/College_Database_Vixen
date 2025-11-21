"""
Update accredagency in Institutions table and
Batch insert data for yearly Students, Financials, and Academics tables

Usage:
    python load-scorecard.py ../data/scorecard/scorecard_2019.csv
    python load-scorecard.py ../data/scorecard/scorecard_2020.csv
    python load-scorecard.py ../data/scorecard/scorecard_2021.csv
    python load-scorecard.py ../data/scorecard/scorecard_2022.csv

Note:
    - Rename csv files to end with 4-digit year, e.g., scorecard_2022.csv
    - Run the script for each year file by chronological order (2019-2022)
"""
from datetime import datetime
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


def error_log(e, i, row, csv_path):
    '''
    Logs an error to error_log.txt
    Input:
        e: Exception, the exception that occurred
        i: int, the row number
        row: tuple, the data row that caused the error
        csv_path: str, path to the CSV file being processed
    '''
    # Append error details to error_log.txt
    with open("error_log.txt", "a") as f:
        f.write(f"Timestamp: {datetime.now()}\n")
        f.write(f"When running load-scorecard.py, Row {i} failed in "
                f"{csv_path}\n")
        f.write(f"ERROR: {e}\n")
        f.write(f"Row data: {row}\n")
        f.write("\n")


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

    # Build accredagency update rows
    accred_rows = []
    for _, r in data.iterrows():
        unitid = r.get("UNITID")
        accred = clean(r.get("ACCREDAGENCY"))
        if unitid is None or str(unitid).strip() == "":
            continue
        accred_rows.append((accred, unitid))

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

    # Connect to the database
    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname="",
        user="",
        password=""
    )
    cursor = conn.cursor()

    # Batch update accredagency in Institutions table
    print("Updating accredagency in Institutions...")
    try:
        cursor.executemany(accredagency_sql, accred_rows)
        conn.commit()
        print(f"Done. Updated {cursor.rowcount} accredagency records.")
    except Exception as e:
        conn.rollback()
        print("[ERROR] accredagency batch update failed:", e)
        error_log(e, "accredagency_batch", "N/A", csv_path)
        sys.exit(1)

    # Fetch valid institution_ids
    cursor.execute("SELECT institution_id FROM Institutions")
    valid_ids = {row[0] for row in cursor.fetchall()}
    students_rows = [row for row in students_rows if row[0] in valid_ids]
    financials_rows = [row for row in financials_rows if row[0] in valid_ids]
    academics_rows = [row for row in academics_rows if row[0] in valid_ids]

    # Insert rows into Students, Financials, and Academics tables
    inserted = {"Students": 0, "Financials": 0, "Academics": 0}

    # Insert into Students table
    print("Inserting into Students...")
    try:
        cursor.executemany(students_sql, students_rows)
        print(f"Inserted {cursor.rowcount} Students rows.")
        inserted["Students"] = cursor.rowcount
    except Exception as e:
        conn.rollback()
        failed_index = cursor.rowcount  # index of failing row
        bad_row = students_rows[failed_index]
        error_log(e, failed_index, bad_row, csv_path)  # Log the error
        sys.exit(1)

    # Insert into Financials table
    print("Inserting into Financials...")
    try:
        cursor.executemany(financials_sql, financials_rows)
        print(f"Inserted {cursor.rowcount} Financials rows.")
        inserted["Financials"] = cursor.rowcount
    except Exception as e:
        conn.rollback()
        failed_index = cursor.rowcount  # index of failing row
        bad_row = financials_rows[failed_index]
        print("[ERROR] Financials batch insert failed:", e)
        print("Failing row index:", failed_index)
        print("Failing row data:", bad_row)
        error_log(e, failed_index, bad_row, csv_path)  # Log the error
        sys.exit(1)
    
    # Insert into Academics table
    print("Inserting into Academics...")
    try:    
        cursor.executemany(academics_sql, academics_rows)
        print(f"Inserted {cursor.rowcount} Academics rows.")
        inserted["Academics"] = cursor.rowcount
    except Exception as e:
        conn.rollback()
        failed_index = cursor.rowcount  # index of failing row
        bad_row = academics_rows[failed_index]
        print("[ERROR] Academics batch insert failed:", e)
        print("Failing row index:", failed_index)
        print("Failing row data:", bad_row)
        error_log(e, failed_index, bad_row, csv_path)  # Log the error
        sys.exit(1)

    # Commit all inserts
    conn.commit()
    cursor.close()
    conn.close()
    print(f"All insertions done for year {year}.")


if __name__ == "__main__":
    main()
