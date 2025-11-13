import psycopg

conn = psycopg.connect(
    host="debprodserver.postgres.database.azure.com",
    dbname="jiashenw",
    user="jiashenw",
    password="h6YwRmN3yt"
)

cur = conn.cursor()
cur.execute(
    "INSERT INTO Institutions (institution_id, name, accredagency, control, CCbasic, region, csba, cba, county_fips, city, state, address, zip_code, latitude, longitude) "
    "VALUES (123456, 'University', 'Agency', 1, 1, 5, 34567, 890, 12345, 'City', 'ST', '123 Example St', 12345, 40.0, 75.0)")

conn.commit()

print("Row inserted successfully.")
cur.close()
conn.close()
