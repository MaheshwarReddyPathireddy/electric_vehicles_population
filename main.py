import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

df = pd.read_csv('/Users/maheshwarreddy/Desktop/python-major-project/Electric_Vehicle_Population_Data.csv')
df.head()
def create_table_model(data_filename,norm_db):
    with open(data_filename) as f:
        data = f.readlines()
    data = [x.strip() for x in data]
    model_year = []
    make = []
    model = []
    Electric_Vehicle_Type = []
    electric_range = []
    cavf = []
    for i in range(1,len(data)):
        model_year.append(data[i].split(',')[5])
        make.append(data[i].split(',')[6])
        model.append(data[i].split(',')[7])
        Electric_Vehicle_Type.append(data[i].split(',')[8])
        cavf.append(data[i].split(',')[9])
        electric_range.append(data[i].split(',')[10])
    vehical_details_table = create_connection(norm_db,delete_db=False)
    create_table_sql = """CREATE TABLE IF NOT EXISTS ModelDetails (
        modelId INTEGER PRIMARY KEY,
        model_year INTEGER,
        make TEXT,
        model TEXT,
        Electric_Vehicle_Type TEXT,
        CAVF TEXT,
        electric_range INTEGER
    );"""
    create_table(vehical_details_table, create_table_sql,'ModelDetails')
    for i in range(len(model_year)):
        sql = """INSERT or Ignore INTO ModelDetails (model_year, make, model, Electric_Vehicle_Type, CAVF,electric_range)
        VALUES (?,?,?,?,?,?)"""
        cur = vehical_details_table.cursor()
        cur.execute(sql, (model_year[i], make[i], model[i], Electric_Vehicle_Type[i], cavf[i], electric_range[i]))
    vehical_details_table.commit()
    vehical_details_table.close()
    return vehical_details_table
create_table_model('/Users/maheshwarreddy/Desktop/python-major-project/Electric_Vehicle_Population_Data.csv', 'norm.db')
def create_modelid_dict(norm_db):
    conn = create_connection(norm_db)
    sql = """SELECT modelId, model_year, make, model FROM ModelDetails"""
    rows = execute_sql_statement(sql, conn)
    modelid_dict = {}
    for row in rows:
        modelid_dict[(row[1],row[2],row[3])] = row[0]
    return modelid_dict
modelid_dict = create_modelid_dict('norm.db')
modelid_dict
def create_table_state(data_filename,norm_db):
    with open(data_filename) as f:
        data = f.readlines()
    data = [x.strip() for x in data]
    state = []
    for i in range(1,len(data)):
        state.append(data[i].split(',')[3])
    state_table = create_connection(norm_db,delete_db=False)
    create_table_sql = """CREATE TABLE IF NOT EXISTS StateDetails (
        stateId INTEGER PRIMARY KEY,
        state TEXT
    );"""
    state = list(set(state))
    state.sort()
    create_table(state_table, create_table_sql,'StateDetails')
    for i in range(len(state)):
        sql = """INSERT or Ignore INTO StateDetails (state)
        VALUES (?)"""
        cur = state_table.cursor()
        cur.execute(sql, (state[i],))
    state_table.commit()
    state_table.close()
    return state_table

create_table_state('/Users/maheshwarreddy/Desktop/python-major-project/Electric_Vehicle_Population_Data.csv', 'norm.db')
def create_state_dict(norm_db):
    conn = create_connection(norm_db)
    sql = """SELECT stateId, state FROM StateDetails"""
    rows = execute_sql_statement(sql, conn)
    state_dict = {}
    for row in rows:
        state_dict[row[1]] = row[0]
    return state_dict

create_state_dict('norm.db')
def create_table_address(data_filename,norm_db):
    with open(data_filename) as f:
        data = f.readlines()
    data = [x.strip() for x in data]
    state = []
    city = []
    county = []
    postal_code = []
    for i in range(1,len(data)):
        county.append(data[i].split(',')[1])
        state.append(data[i].split(',')[3])
        city.append(data[i].split(',')[2])
        postal_code.append(data[i].split(',')[4])
    stateids = create_state_dict('norm.db')
    address_table = create_connection(norm_db,delete_db=False)
    create_table_sql = """CREATE TABLE IF NOT EXISTS Address (
        addressId INTEGER PRIMARY KEY,
        county TEXT,
        state TEXT,
        city TEXT,
        postal_code TEXT,
        stateId INTEGER NOT NULL,
        FOREIGN KEY (stateId) REFERENCES StateDetails (stateId)
    );"""
    create_table(address_table, create_table_sql,'Address')
    for i in range(len(state)):
        execute_sql_statement(
            """INSERT or Ignore INTO Address (addressId, county, state, city, postal_code,stateId)
            VALUES (%d,"%s","%s","%s","%s",%d);""" % (i+1, county[i], state[i], city[i], postal_code[i],stateids[state[i]]), address_table)
    address_table.commit()
    address_table.close()
    return address_table


create_table_address('/Users/maheshwarreddy/Desktop/python-major-project/Electric_Vehicle_Population_Data.csv', 'norm.db')

def create_address_dict(norm_db):
    conn = create_connection(norm_db)
    sql = """SELECT addressId, county, state, city, postal_code FROM Address"""
    rows = execute_sql_statement(sql, conn)
    address_dict = {}
    for row in rows:
        address_dict[(row[1],row[2],row[3],row[4])] = row[0]
    return address_dict

create_address_dict('norm.db')
def create_base_table(date_filename, norm_db):
    conn = create_connection(norm_db)
    with open(date_filename) as f:
        data = f.readlines()
    data = [x.strip() for x in data]
    DOLId = []
    for i in range(1, len(data)):
        DOLId.append(data[i].split(',')[13])
    base_table = create_connection(norm_db, delete_db=False)
    create_table_sql = """CREATE TABLE IF NOT EXISTS BaseTable (
        DOLId INTEGER PRIMARY KEY,
        modelId INTEGER NOT NULL,
        addressId INTEGER NOT NULL,
        FOREIGN KEY (modelId) REFERENCES ModelDetails (modelId),
        FOREIGN KEY (addressId) REFERENCES Address (addressId)
    );"""
    create_table(base_table, create_table_sql, 'BaseTable')
    modelids = create_modelid_dict('norm.db')
    addressids = create_address_dict('norm.db')
    
    # Print the keys in the dictionaries for debugging
    print("Model IDs keys:", modelids.keys())
    print("Address IDs keys:", addressids.keys())
    
    for i in range(len(DOLId)):
        model_key = (data[i].split(',')[0], data[i].split(',')[5], data[i].split(',')[6])
        address_key = (data[i].split(',')[1], data[i].split(',')[3], data[i].split(',')[2], data[i].split(',')[4])
        
        # Print the keys being used for debugging
        print("Model key being used:", model_key)
        print("Address key being used:", address_key)
        
        try:
            execute_sql_statement(
                """INSERT or Ignore INTO BaseTable (DOLId, modelId, addressId)
                VALUES (%d,%d,%d);""" % (int(DOLId[i]), modelids[model_key], addressids[address_key]), base_table)
        except KeyError as e:
            print(f"KeyError: {e} not found in dictionaries")
            continue
    
    base_table.commit()
    base_table.close()
    return base_table

create_base_table('/Users/maheshwarreddy/Desktop/python-major-project/Electric_Vehicle_Population_Data.csv', 'norm.db')
def create_station_table(date_filename,norm_db):
    conn = create_connection(norm_db)
    with open(date_filename) as f:
        data = f.readlines()
    data = [x.strip() for x in data]
    year = []
    state = []
    electric_stations = []
    for i in range(1,len(data)):
        state.append(data[i].split(',')[1])
        year.append(data[i].split(',')[0])
        electric_stations.append(data[i].split(',')[2])
    #map state to statecode
    states = {'Alaska':'AK','Alabama':'AL','Arkansas':'AR','Arizona':'AZ','California':'CA','Colorado':'CO','Connecticut':'CT','District of Columbia':'DC','Delaware':'DE','Florida':'FL','Georgia':'GA','Hawaii':'HI','Iowa':'IA','Idaho':'ID','Illinois':'IL','Indiana':'IN','Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Massachusetts':'MA','Maryland':'MD','Maine':'ME','Michigan':'MI','Minnesota':'MN','Missouri':'MO','Mississippi':'MS','Montana':'MT','North Carolina':'NC','North Dakota':'ND','Nebraska':'NE','New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','Nevada':'NV','New York':'NY','Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC','South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Virginia':'VA','Vermont':'VT','Washington':'WA','Wisconsin':'WI','West Virginia':'WV','Wyoming':'WY'}
    for i in range(len(state)):
        state[i] = states[state[i]]
    # print(state)
    stateids = create_state_dict('norm.db')
    station_table = create_connection(norm_db,delete_db=False)
    create_table_sql = """CREATE TABLE IF NOT EXISTS StationCount (
        stationId INTEGER PRIMARY KEY,
        year INTEGER,
        state TEXT,
        stateId INTEGER NOT NULL,
        electric_stations INTEGER
    );"""
    stateid = {'AK': 0,
 'AL': 1,
 'AR': 2,
 'AZ': 3,
 'CA': 4,
 'CO': 5,
 'CT': 6,
 'DC': 7,
 'DE': 8,
 'FL': 9,
 'GA': 10,
 'HI': 11,
 'IA': 12,
 'ID': 13,
 'IL': 14,
 'IN': 15,
 'KS': 16,
 'KY': 17,
 'LA': 18,
 'MA': 19,
 'MD': 20,
 'ME': 21,
 'MI': 22,
 'MN': 23,
 'MO': 24,
 'MS': 25,
 'MT': 26,
 'NC': 27,
 'ND': 28,
 'NE': 29,
 'NH': 30,
 'NJ': 31,
 'NM': 32,
 'NV': 33,
 'NY': 34,
 'OH': 35,
 'OK': 36,
 'OR': 37,
 'PA': 38,
 'RI': 39,
 'SC': 40,
 'SD': 41,
 'TN': 42,
 'TX': 43,
 'UT': 44,
 'VA': 45,
 'VT': 46,
 'WA': 47,
 'WI': 48,
 'WV': 49,
 'WY': 50}
    create_table(station_table, create_table_sql,'StationCount')
    for i in range(len(state)):
        execute_sql_statement(
            """INSERT or Ignore INTO StationCount (stationId, year, state, stateId, electric_stations)
            VALUES (%d,%d,"%s",%d,%d);""" % (i+1, int(year[i]), state[i],stateid[state[i]],int(electric_stations[i])), station_table)
    station_table.commit()
    station_table.close()
    return station_table
    


create_station_table('/Users/maheshwarreddy/Desktop/python-major-project/Station_Count 2.csv', 'norm.db')
import matplotlib.pyplot as plt
import numpy as np

# Simulated years from 1950 to 2100
years = np.arange(1950, 2101, 1)

# Example data (replace with actual data)
india_rate = np.concatenate([np.random.normal(2, 0.5, 50), np.linspace(1.5, -0.5, 101)])
southern_asia_rate = np.concatenate([np.random.normal(1.5, 0.3, 50), np.linspace(1.2, -0.4, 101)])
asia_rate = np.concatenate([np.random.normal(1.3, 0.2, 50), np.linspace(1, -0.5, 101)])

# Prediction intervals (using random deviations for illustration)
upper_bound = india_rate + np.random.normal(0.1, 0.05, len(years))
lower_bound = india_rate - np.random.normal(0.1, 0.05, len(years))

# Create the plot
plt.figure(figsize=(10, 6))

# Plot India, Southern Asia, and Asia lines
plt.plot(years, india_rate, label='India', color='blue')
plt.plot(years, southern_asia_rate, label='Southern Asia', color='green')
plt.plot(years, asia_rate, label='Asia', color='red')

# Fill the prediction interval for India
plt.fill_between(years, lower_bound, upper_bound, color='blue', alpha=0.2, label="95% prediction interval")

# Labels and Title
plt.title('India: Annual Rate of Population Change (1950-2100)', fontsize=14)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Percent', fontsize=12)

# Show legend
plt.legend()

# Display grid and plot
plt.grid(True)
plt.show()

