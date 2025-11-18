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
    if isinstance(value, str):
        v = value.strip()
        if v in {"", "NA", "N/A", "NULL"}:
            return None
        if v in {"-3", "-2", "-999"}:
            return None
        return v
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
        clean(rec.get("UNITID")),
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
        host="debprodserver.postgres.database.azure.com",
        dbname="",
        user="",
        password=""
    )
    cursor = conn.cursor()
    inserted = 0
    updated = 0

    # Insert/Update rows
    print("Inserting/Updating rows in Institutions...")
    for i, row in enumerate(rows, start=1):
        # First try to update
        # Update row is shifted by 1 for institution_id at the end
        update_row = row[1:] + (row[0],)
        try:
            cursor.execute(update_sql, update_row)
            if cursor.rowcount == 0:
                # If no rows were updated, insert
                cursor.execute(insert_sql, row)
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Row {i} failed: {e}")
            # Create an error log file
            error_log(e, i, row, csv_path)
            sys.exit(1)
        if i % 500 == 0:
            print(f"{i} rows inserted/updated...")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done. {inserted} of {len(rows)} records inserted and "
          f"{updated} records updated in Institutions successfully.")


if __name__ == "__main__":
    main()
