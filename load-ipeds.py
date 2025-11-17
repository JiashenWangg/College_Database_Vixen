"""
Command-line ETL loader for the IPEDS "hd2022.csv" header file -> Institutions

Call:
    python load-ipeds.py ../data/ipeds/hd2022.csv
"""
import sys
import pandas as pd
import psycopg


COLUMN_MAP = {
    "institution_id": "UNITID",
    "name": "INSTNM",
    "accredagency": "",  # Not available in hd2022.csv
    "control": "CONTROL",
    "CCbasic": "C21BASIC",
    "region": "OBEREG",
    "csba": "CBSA",
    "cba": "CSA",
    "county_fips": "COUNTYCD",
    "city": "CITY",
    "state": "STABBR",
    "address": "ADDR",
    "zip": "ZIP",
    "latitude": "LATITUDE",
    "longitude": "LONGITUD",
}


def clean(value):
    '''
    Cleans a value from the IPEDS dataset
    by converting known missing value indicators to None.
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
    '''Converts a record from IPEDS into a tuple for insertion'''
    # Find the correct columns for institution_id and name
    uid_col = next((c for c in rec.keys() if "UNITID" in c.upper()), None)
    instnm_col = next((c for c in rec.keys() if "INSTNM" in c.upper()), None)
    return (
        clean(rec.get(uid_col)),
        clean(rec.get(instnm_col)),
        None,
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

    # Connect to the database
    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname="",
        user="",
        password=""
    )
    cursor = conn.cursor()
    inserted = 0

    # Insert rows
    with conn.transaction():
        for i, row in enumerate(rows, start=1):
            try:
                with conn.transaction():
                    cursor.execute(insert_sql, row)
                    if i % 500 == 0:
                        print(f"{i} rows inserted...")
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
