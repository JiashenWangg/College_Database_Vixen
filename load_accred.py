"""
Update accredagency in Institutions from scorecard_2022.csv

Usage:
    python load_accred.py ../data/scorecard/scorecard_2022.csv
"""

import sys
import pandas as pd
import psycopg


def main():
    # Load the CSV file
    csv_path = sys.argv[1]
    data = pd.read_csv(csv_path, dtype=str, low_memory=False, 
                       encoding="latin1")
    data.columns = [c.strip().upper() for c in data.columns]
    print(f"Loaded {len(data)} rows from {csv_path}")

    update_sql = """
        UPDATE Institutions
        SET accredagency = %s
        WHERE institution_id = %s;
    """

    # Connect to the database
    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname="",
        user="",
        password=""
    )
    cursor = conn.cursor()
    updated = 0
    try:
        # Update rows
        for i, row in data.iterrows():
            accredagency = row["ACCREDAGENCY"]
            unitid = row["UNITID"]

            # Skip blanks
            if not isinstance(unitid, str) or unitid.strip() == "":
                continue

            try:
                cursor.execute(update_sql, (accredagency, unitid))
                if cursor.rowcount > 0:
                    updated += cursor.rowcount
                if (i + 1) % 500 == 0:
                    print(f"Processed {i + 1} rows...")
            except Exception as e:
                print(f"[ERROR] Row {i+1} (UNITID={unitid}) failed: {e}")
                raise
        conn.commit()
        print(f"Done. {updated} records of accredagency in Institutions "
              f"updated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Transaction failed: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
