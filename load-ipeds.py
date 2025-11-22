"""
Load IPEDS data into Institutions table
If a record with the same institution_id exists, update it;
otherwise, insert a new record.

Call:
    (optional) python load-ipeds.py ../data/ipeds/hd2019.csv
    (optional) python load-ipeds.py ../data/ipeds/hd2020.csv
    (optional) python load-ipeds.py ../data/ipeds/hd2021.csv
    python load-ipeds.py ../data/ipeds/hd2022.csv
Note:
    - If you have multiple years of IPEDS data, run them in order
"""
from datetime import datetime
import sys
import pandas as pd
import psycopg
import credentials


def clean(value):
    '''
    Cleans a value from the IPEDS dataset
    by converting known missing value indicators to None
    Input:
        value: any
    Output:
        value or None
    '''
    if value is None:
        return None
    # Handle string missing value indicators
    if isinstance(value, str):
        v = value.strip()
        if v in {"", "NA", "N/A", "NULL"}:
            return None
        if v in {"-3", "-2", "-999"}:
            return None
        return v
    # Handle negative numbers
    if isinstance(value, (int, float)) and value < 0:
        return None
    return value


def to_row(rec):
    '''
    Converts a record from IPEDS into a tuple for insertion
    Input:
        rec: dict, a record from the IPEDS dataset
    Output:
        tuple, a row for Institutions table
    '''
    return (
        clean(int(rec.get("UNITID"))),
        clean(rec.get("INSTNM")),
        None,  # accredagency to be updated later in load-scorecard.py
        clean(rec.get("CONTROL")),
        clean(rec.get("C21BASIC")),
        clean(rec.get("OBEREG")),
        clean(rec.get("CBSA")),
        clean(rec.get("CSA")),
        clean(rec.get("COUNTYCD")),
        clean(rec.get("CITY")),
        clean(rec.get("STABBR")),
        clean(rec.get("ADDR")),
        clean(rec.get("ZIP")),
        clean(rec.get("LATITUDE")),
        clean(rec.get("LONGITUD"))
    )


def error_log(e, i, row, csv_path):
    '''
    Logs an error to error_log.txt
    Input:
        e: Exception, the exception that occurred
        i: int, the row number
        row: tuple, the data row that caused the error
        csv_path: str, path to the CSV file being processed
    '''
    with open("error_log.txt", "a") as f:
        f.write(f"Timestamp: {datetime.now()}\n")
        f.write(f"When running load-ipeds.py, Row {i} failed in {csv_path}\n")
        f.write(f"ERROR: {e}\n")
        f.write(f"Row data: {row}\n")
        f.write("\n")


def main():
    # Load the CSV file
    csv_path = sys.argv[1]
    data = pd.read_csv(csv_path, dtype=str, low_memory=False,
                       encoding="latin1")
    data.columns = [c.strip().upper() for c in data.columns]

    # Convert to rows
    rows = [to_row(rec) for rec in data.to_dict(orient="records")]
    print(f"Loaded {len(rows)} rows from {csv_path}")

    insert_sql = (
        "INSERT INTO Institutions (institution_id, name, accredagency, "
        "control, CCbasic, region, csba, cba, county_fips, city, state, "
        "address, zip, latitude, longitude) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    update_sql = (
        "UPDATE Institutions SET name = %s, accredagency = %s, control = %s, "
        "CCbasic = %s, region = %s, csba = %s, cba = %s, county_fips = %s, "
        "city = %s, state = %s, address = %s, zip = %s, latitude = %s, "
        "longitude = %s WHERE institution_id = %s")

    # Connect to the database
    conn = psycopg.connect(
        host=credentials.DB_HOST,
        dbname=credentials.DB_NAME,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cursor = conn.cursor()

    update_rows = []
    insert_rows = []

    # --- Separate rows into UPDATE vs INSERT ---
    print("Determining which rows to update vs insert...")
    cursor.execute("SELECT institution_id FROM Institutions;")
    existing_ids = {row[0] for row in cursor.fetchall()}
    for row in rows:
        inst_id = row[0]
        if inst_id in existing_ids:
            # UPDATE row: all values except inst_id, then inst_id at end
            update_rows.append(row[1:] + (inst_id,))
        else:
            insert_rows.append(row)
    print(f"Rows to UPDATE: {len(update_rows)}")
    print(f"Rows to INSERT: {len(insert_rows)}")

    # Updating rows in Institutions table
    print("Updating rows in Institutions...")
    updated = 0
    if update_rows:
        try:
            cursor.executemany(update_sql, update_rows)
            updated = cursor.rowcount
            print(f"Updated {updated} rows.")
        except Exception as e:
            conn.rollback()
            failed_index = cursor.rowcount  # index of failing row
            bad_row = update_rows[failed_index]
            # Find the failing index in the original data
            for idx, r in enumerate(rows):
                if r[0] == bad_row[-1]:  # match institution_id
                    failed_index = idx + 1  # +1 for 1-based index
                    break
            print("[ERROR] Batch update failed:", e)
            print("Failing row index:", failed_index)
            print("Failing row data:", bad_row)
            error_log(e, failed_index, bad_row, csv_path)  # Log the error
            sys.exit(1)

    # Inserting rows into Institutions table
    print("Inserting rows into Institutions...")
    inserted = 0
    if insert_rows:
        try:
            cursor.executemany(insert_sql, insert_rows)
            inserted = cursor.rowcount
            print(f"Inserted {inserted} rows.")
        except Exception as e:
            conn.rollback()
            failed_index = cursor.rowcount  # index of failing row
            bad_row = insert_rows[failed_index]
            # Find the failing index in the original data
            for idx, r in enumerate(rows):
                if r[0] == bad_row[0]:  # match institution_id
                    failed_index = idx + 1  # +1 for 1-based index
                    break
            print("[ERROR] Batch insert failed:", e)
            print("Failing row index:", failed_index)
            print("Failing row data:", bad_row)
            error_log(e, failed_index, bad_row, csv_path)  # Log the error
            sys.exit(1)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done. {inserted} of {len(rows)} records inserted and "
          f"{updated} records updated in Institutions successfully.")


if __name__ == "__main__":
    main()
