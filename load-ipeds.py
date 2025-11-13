import psycopg2
import pandas as pd
import sys


# Load data from source file
def load_data():

    # Get file path from command line
    path_file = sys.argv[1]

    try:
        data = pd.read_csv(path_file, encoding='latin-1')  # or try 'cp1252' if latin-1 doesn't work
        
        # Select only the columns we need for the Institutions table
        columns_to_select = ['UNITID', 'INSTNM', 'INSTNM', 'CONTROL', 'CCBASIC', 'OBEREG',
                             'CBSA', 'CSA', 'COUNTYCD', 'CITY', 'STABBR',
                             'ADDR', 'ZIP', 'LATITUDE', 'LONGITUD']
        
        # Select the columns and handle missing values
        selected_data = data[columns_to_select].copy()
        
        return selected_data

    except Exception as e:
        print("Error occurred loading data: ", e)
        raise


def insert_data(df):

    # Connect to database
    print("In the function")
    conn = psycopg2.connect(
        host="debprodserver.postgres.database.azure.com",
        database="pramitv",
        user="pramitv",
        password = "HbALuVdRxq")

    print("In the function")
    # Create cursor object
    cursor = conn.cursor()
    print("connected")
    # Use transaction
    try:
        conn.autocommit = False
        print("Starting trans")
        cursor.executemany(
            "INSERT INTO Institutions (institution_id, name, accredagency, control,"
            "CCbasic, region, cbsa, csa, county_fips, city, state, "
            "address, zip, latitude, longitude) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s)",
            df.values.tolist())
        cursor.execute("SELECT COUNT(*) FROM Institutions")
        print("executed")
        conn.commit()

    except Exception as e:
        print("Error inserting data: ", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    df = load_data()
    print(df.columns)
    print("Done loading")
    insert_data(df)
