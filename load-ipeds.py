"""
Load IPEDS HD2022 data into Institutions table

Call:
    python load-ipeds.py ../data/ipeds/hd2022.csv
Note:
    - The csv file should be the most recent HD data (hd2022.csv)
"""
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
    print("Inserting rows into Institutions...")
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
    print(f"Done. {inserted} of {len(rows)} records inserted into "
          f"Institutions successfully.")


if __name__ == "__main__":
    main()
