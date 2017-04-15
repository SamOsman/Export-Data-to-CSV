import csv
import sqlite3

sqlite_file = 'sensor-data-tables.db'
table_a = "single_output_sensors"
table_b = "Accelerometer"
table_c = "Compass"
table_d = "GPS"

# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()

# connection to csv files for every table
try:
    csvFile1 = open('csv/single-output-data.csv', 'w')
    csvFile2 = open('csv/Accelerometer.csv', 'w')
    csvFile3 = open('csv/Compass.csv', 'w')
    csvFile4 = open('csv/GPS.csv', 'w')

except IOError as ioe:
    e = "can't open csv file"
    print(e)
    with open("error-log.txt", 'a') as log:
        log.write(e)

write1 = csv.writer(csvFile1)
write2 = csv.writer(csvFile2)
write3 = csv.writer(csvFile3)
write4 = csv.writer(csvFile4)

# Retrieve all column information (Column Headers) for every table
cursor.execute('PRAGMA TABLE_INFO({})'.format(table_a))
heading_a = [tup[1] for tup in cursor.fetchall()]

cursor.execute('PRAGMA TABLE_INFO({})'.format(table_b))
heading_b = [tup[1] for tup in cursor.fetchall()]

cursor.execute('PRAGMA TABLE_INFO({})'.format(table_c))
heading_c = [tup[1] for tup in cursor.fetchall()]

cursor.execute('PRAGMA TABLE_INFO({})'.format(table_d))
heading_d = [tup[1] for tup in cursor.fetchall()]


# single output sensors--------------------------------------------------

# write column header
write1.writerow([heading_a[0], heading_a[1], heading_a[2], heading_a[3], heading_a[4], heading_a[5], heading_a[6],
                heading_a[7], heading_a[8], heading_a[9]])

# Retrieve all data from table
cursor = conn.execute("SELECT *  from %s " % table_a)

# iterate through all the rows of data and record them
for row in cursor:
    write1.writerow([int(row[0]), str(row[1]), int(row[2]), str(row[3]), int(row[4]), float(row[5]), int(row[6]),
                    float(row[7]), float(row[8]), float(row[9])])


# Accelerometer----------------------------------------------------------

write2.writerow([heading_b[0], heading_b[1], heading_b[2], heading_b[3], heading_b[4], heading_b[5], heading_b[6],
                heading_b[7]])

cursor = conn.execute("SELECT *  from %s " % table_b)

for row in cursor:
    write2.writerow([int(row[0]), str(row[1]), int(row[2]), int(row[3]), int(row[4]), float(row[5]),
                    float(row[6]), float(row[7])])


# Compass-----------------------------------------------------------------

write3.writerow([heading_c[0], heading_c[1], heading_c[2], heading_c[3]])

cursor = conn.execute("SELECT *  from %s " % table_c)

for row in cursor:
    write3.writerow([int(row[0]), str(row[1]), str(row[2]), float(row[3])])


# GPS-----------------------------------------------------------------

write4.writerow([heading_d[0], heading_d[1], heading_d[2], heading_d[3]])

cursor = conn.execute("SELECT *  from %s " % table_d)

for row in cursor:
    write4.writerow([int(row[0]), str(row[1]), float(row[2]), float(row[3])])













