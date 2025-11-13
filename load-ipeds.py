"""
Command-line ETL loader for the IPEDS "hd2022.csv" header file -> Institutions

Call:
    python load-ipeds.py path/hd2022.csv.csv
"""
import sys
import pandas as pd
import numpy as np
import psycopg


COLUMN_MAP = {
    "institution_id": "OPEID",
    "name": "INSTNM",
    "accredagency": "IALIAS",  # Not in csv; using IALIAS for placeholder
    "control": "CONTROL",
    "CCbasic": "C21BASIC",
    "region": "OBEREG",
    "csba": "CBSA",
    "cba": "CSA",
    "county_fips": "COUNTYCD",
    "city": "CITY",
    "state": "STABBR",
    "address": "ADDR",
    "zip_code": "ZIP",
    "latitude": "LATITUDE",
    "longitude": "LONGITUD",
}


def clean(value):
    """Convert -999, blanks, NA, etc. to None"""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if v in {"", "NA", "N/A", "NULL", "PrivacySuppressed"}:
            return None
        return v
    try:
        if np.isnan(value):
            return None
        # Handle numeric values that are -999 or similar
        if value == -999:
            return None
    except Exception:
        pass
    return value


def to_row(rec):
    """Convert a line from CSV into a tuple for insertion"""
    return (
        clean(rec.get("OPEID")),
        clean(rec.get("INSTNM")),
        clean(rec["IALIAS"]),
        clean(rec["CONTROL"]),
        clean(rec["C21BASIC"]),
        clean(rec["OBEREG"]),
        clean(rec["CBSA"]),
        clean(rec["CSA"]),
        clean(rec["COUNTYCD"]),
        clean(rec["CITY"]),
        clean(rec["STABBR"]),
        clean(rec["ADDR"]),
        clean(rec["ZIP"]),
        clean(rec["LATITUDE"]),
        clean(rec["LONGITUD"]),
    )


def main():

    if len(sys.argv) != 2:
        print("Usage: python load-ipeds.py path/to/hd2022.csv")
        sys.exit(2)

    csv_path = sys.argv[1]
    data = pd.read_csv(csv_path, dtype=str, low_memory=False, encoding="latin1")
    data.columns = [c.strip().upper() for c in data.columns]

    rows = [to_row(rec) for rec in data.to_dict(orient="records")]
    print(f"Loaded {len(rows)} rows from {csv_path}")

    insert_sql = (
        "INSERT INTO Institutions (institution_id, name, accredagency, control, CCbasic, region, csba, cba, county_fips, city, state, address, zip, latitude, longitude)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    '''
    example = (123456, "Example University", "Some Agency", 1, 15, 5,
               34567, 890, 12345, "Sample City", "ST", "123 Example St", "12345", 40.0, -75.0)
    '''
    inserted = 0

    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname="agehr",
        user="agehr",
        password="?eMc2GnHzV"
    )

    cursor = conn.cursor()

    with conn.transaction():
        for i, row in enumerate(rows, start=1):
            try:
                with conn.transaction():
                    cursor.execute(insert_sql, row)
                    inserted += 1
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Row {i} failed: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done. {inserted} of {len(rows)} rows inserted successfully.")


if __name__ == "__main__":
    main()
