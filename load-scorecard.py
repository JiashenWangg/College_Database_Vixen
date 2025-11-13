import psycopg
import pandas as pd
import sys


# Load data from source file
def load_data():

    # Get file path from command line
    path_file = sys.argv[1]

    try:
        data = pd.read_csv(path_file)
        return data

    except Exception as e:
        print("Error occurred loading data: ", e)
        raise


def insert_data(df):

    # Connect to database
    conn = psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname=dbname,
        user=username,
        password=password)

    # Create cursor object
    cursor = conn.cursor()

    # Use transaction
    try:
        with conn.transaction():

            cursor.executemany("INSERT INTO Institutions (institution_id, name, accredagency, control, CCbasic)"
                               "VALUES (%s, %s, %s, %s, %s)", df.values.tolist())
            cursor.execute("SELECT COUNT(*) FROM Institutions")

    except Exception as e:
        print("Error inserting data: ", e)

    finally:
        cursor.close()
        conn.close()
