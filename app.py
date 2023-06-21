from flask import Flask, render_template, request
import csv
import pyodbc
import time
import os
import sqlite3
import pymemcache
import redis
from flask_caching import Cache

app = Flask(__name__)

# Define the Azure SQL Database connection details
server = 'txt6312server.database.windows.net'
database = 'test'
username = 'CloudSA3d95adf8'
password = 'tiger@123TT'
driver = '{ODBC Driver 18 for SQL Server}'

# Connect to the Azure SQL Database
conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# cache_memcached = pymemcache.Client(('localhost', 11211))
cache_redis = redis.Redis(host='localhost', port=6379, db=0)

# Define the Flask-Caching configuration
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# Define the main route
@app.route('/')
def index():
    return render_template('index.html')

# Route to handle creating the SQL table and calculating time
@app.route('/create_table', methods=['POST'])
def create_table():
    start_time = time.time()

    # Create the SQL table in Azure SQL Database
    cursor.execute('''IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.earthquakes') AND type in (N'U'))
                      CREATE TABLE dbo.earthquakes (
                          time VARCHAR(255),
                          latitude FLOAT,
                          longitude FLOAT,
                          depth FLOAT,
                          mag FLOAT,
                          magType VARCHAR(255),
                          nst FLOAT,
                          gap FLOAT,
                          dmin FLOAT,
                          rms FLOAT,
                          net VARCHAR(255),
                          id VARCHAR(255),
                          updated VARCHAR(255),
                          place VARCHAR(255),
                          type VARCHAR(255),
                          horizontalError FLOAT,
                          depthError FLOAT,
                          magError FLOAT,
                          magNst FLOAT,
                          status VARCHAR(255),
                          locationSource VARCHAR(255),
                          magSource VARCHAR(255)
                      )''')

    # Create an index on the location column in Azure SQL Database
    cursor.execute('''IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_location' AND object_id = OBJECT_ID(N'dbo.earthquakes'))
                      CREATE INDEX idx_location ON dbo.earthquakes (place)''')

    conn.commit()

    # Create the SQL table in SQLite database (backup)
    sqlite_conn = sqlite3.connect('earthquakes.db')
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute('''CREATE TABLE IF NOT EXISTS earthquakes (
                                time TEXT,
                                latitude REAL,
                                longitude REAL,
                                depth REAL,
                                mag FLOAT,
                                magType TEXT,
                                nst REAL,
                                gap REAL,
                                dmin REAL,
                                rms REAL,
                                net TEXT,
                                id TEXT,
                                updated TEXT,
                                place TEXT,
                                type TEXT,
                                horizontalError REAL,
                                depthError REAL,
                                magError REAL,
                                magNst REAL,
                                status TEXT,
                                locationSource TEXT,
                                magSource TEXT
                            )''')

    # Create an index on the location column in SQLite database (backup)
    sqlite_cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON earthquakes (place)')

    sqlite_conn.commit()
    sqlite_conn.close()

    end_time = time.time()
    elapsed_time = end_time - start_time

    return render_template('create_table.html', elapsed_time=elapsed_time)

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    file = request.files['csv_file']
    filename = file.filename

    # Save the uploaded CSV file
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(file_path)

    # Read the CSV file and insert data into the Azure SQL Database table
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = []
        for row in reader:
            rows.append((
                row['time'],
                row['latitude'],
                row['longitude'],
                row['depth'],
                row['mag'],
                row['magType'],
                row['nst'],
                row['gap'],
                row['dmin'],
                row['rms'],
                row['net'],
                row['id'],
                row['updated'],
                row['place'],
                row['type'],
                row['horizontalError'],
                row['depthError'],
                row['magError'],
                row['magNst'],
                row['status'],
                row['locationSource'],
                row['magSource']
            ))

        # Check if the 'earthquakes' table already exists
        table_exists = cursor.tables(table='earthquakes').fetchone()
        if table_exists:
            # Table already exists, directly insert data into it
            insert_query = '''INSERT INTO earthquakes (
                                [time], latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place,
                                [type], horizontalError, depthError, magError, magNst, [status], locationSource, magSource
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        else:
            # Table does not exist, create a new one and insert data into it
            cursor.execute('''CREATE TABLE earthquakes (
                                time TEXT,
                                latitude REAL,
                                longitude REAL,
                                depth REAL,
                                mag REAL,
                                magType TEXT,
                                nst REAL,
                                gap REAL,
                                dmin REAL,
                                rms REAL,
                                net TEXT,
                                id TEXT,
                                updated TEXT,
                                place TEXT,
                                type TEXT,
                                horizontalError REAL,
                                depthError REAL,
                                magError REAL,
                                magNst REAL,
                                status TEXT,
                                locationSource TEXT,
                                magSource TEXT
                            )''')
            conn.commit()

            insert_query = '''INSERT INTO earthquakes (
                                [time], latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place,
                                [type], horizontalError, depthError, magError, magNst, [status], locationSource, magSource
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

        cursor.executemany(insert_query, rows)
        conn.commit()

    # Insert data into the SQLite database (backup)
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = []
        for row in reader:
            rows.append((
                row['time'],
                row['latitude'],
                row['longitude'],
                row['depth'],
                row['mag'],
                row['magType'],
                row['nst'],
                row['gap'],
                row['dmin'],
                row['rms'],
                row['net'],
                row['id'],
                row['updated'],
                row['place'],
                row['type'],
                row['horizontalError'],
                row['depthError'],
                row['magError'],
                row['magNst'],
                row['status'],
                row['locationSource'],
                row['magSource']
            ))

        sqlite_conn = sqlite3.connect('earthquakes.db')
        sqlite_cursor = sqlite_conn.cursor()

        # Create the SQLite table if it does not exist
        sqlite_cursor.execute('''CREATE TABLE IF NOT EXISTS earthquakes (
                                    time TEXT,
                                    latitude REAL,
                                    longitude REAL,
                                    depth REAL,
                                    mag REAL,
                                    magType TEXT,
                                    nst REAL,
                                    gap REAL,
                                    dmin REAL,
                                    rms REAL,
                                    net TEXT,
                                    id TEXT,
                                    updated TEXT,
                                    place TEXT,
                                    type TEXT,
                                    horizontalError REAL,
                                    depthError REAL,
                                    magError REAL,
                                    magNst REAL,
                                    status TEXT,
                                    locationSource TEXT,
                                    magSource TEXT
                                )''')
        sqlite_conn.commit()

        insert_query = '''INSERT INTO earthquakes (
                            [time], latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place,
                            [type], horizontalError, depthError, magError, magNst, [status], locationSource, magSource
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        sqlite_cursor.executemany(insert_query, rows)
        sqlite_conn.commit()
        sqlite_conn.close()

    message = "File uploaded successfully and data inserted into the table."
    return render_template('index.html', message=message)

# Route to handle random queries
@app.route('/random_queries', methods=['POST'])
def random_queries():
    num_queries = int(request.form['num_queries'])

    start_time = time.time()

    results = []
    for _ in range(num_queries):
        # Perform a random query in Azure SQL Database
        cursor.execute('SELECT TOP 1 * FROM dbo.earthquakes ORDER BY NEWID()')
        result = cursor.fetchone()
        results.append(result)

    end_time = time.time()
    elapsed_time = end_time - start_time

    return render_template('random_queries.html', results=results, elapsed_time=elapsed_time)

@app.route('/restricted_queries', methods=['POST'])
def restricted_queries():
    magnitude = float(request.form['magnitude_range'])

    # Check if the results are already cached
    cache_key = f'restricted_query_mag_{magnitude}'
    results = cache.get(cache_key)

    if results is None:
        # Results not found in cache, execute the query
        start_time = time.time()

        # Perform the restricted query in Azure SQL Database based on the provided magnitude
        query = '''SELECT * FROM dbo.earthquakes 
                   WHERE mag > ?'''
        cursor.execute(query, (magnitude,))
        results = cursor.fetchall()

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Store the results in cache for future use
        cache.set(cache_key, results)

        print(results)
        return render_template('restricted_queries.html', results=results, elapsed_time=elapsed_time)
    else:
        # Results found in cache, return them directly
        return render_template('restricted_queries.html', results=results, elapsed_time=0)



if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = 'uploads'
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])
    app.run(debug=True,port = 8080)
