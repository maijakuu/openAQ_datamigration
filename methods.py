import contextlib
import os
import psycopg2
import contextlib
import glob
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

@contextlib.contextmanager
def get_connection():
    conn = None
    try:
        conn = psycopg2.connect(f"dbname={os.getenv('DB')} user={os.getenv('DB_USER')} password={os.getenv('DB_PWD')}")
        yield conn
    finally:
        if conn is not None:
            conn.close()

def populate_locations(folder_path="data"): #osoitetaan kansio, missä filut on
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            cur.execute("DELETE FROM locations;")
            conn.commit()
            files = glob.glob(f"{folder_path}/*.csv") 
            #^löytää kaikki CSV-filut määrätyssä kansiossa
            df_list = [pd.read_csv(file) for file in files]
            #^lukee jokaisen CSV:n dataframeksi
            df = pd.concat(df_list, ignore_index=True)
            #^yhdistää nämä kaikki yksittäiset dataframet yhdeksi isoksi dataframeksi

            locations_df = df[['location_id','lat','lon','location','city','country']].drop_duplicates()

            records = locations_df.to_records(index=False)
            execute_values(
                cur,
                '''
                INSERT INTO locations (location_id, lat, lon, location, city, country)
                VALUES %s
                ''',
                records
            )
        conn.commit()

def populate_sensors(folder_path="data"):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            cur.execute("DELETE FROM sensors;")
            conn.commit()
            files = glob.glob(f"{folder_path}/*.csv")
            df_list = [pd.read_csv(file) for file in files]
            df = pd.concat(df_list, ignore_index=True)
  
            sensors_df = df[['sensors_id','parameter','units']].drop_duplicates()

            records = sensors_df.to_records(index=False)#massasiirto
            execute_values(
                cur,
                '''
                INSERT INTO sensors (sensors_id, parameter, units) 
                VALUES %s
                ''',
                records
            )
        conn.commit()

def populate_measurements(folder_path="data"):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;")
            conn.commit()

            files = glob.glob(f"{folder_path}/*.csv")
            df_list = [pd.read_csv(file) for file in files]
            df = pd.concat(df_list, ignore_index=True)
  
            measurements_df = df[['location_id','sensors_id', 'datetime', 'value']].drop_duplicates()
            measurements_df['datetime'] = pd.to_datetime(measurements_df['datetime'])

            records = measurements_df.to_records(index=False)
            execute_values(
                cur,
                '''
                INSERT INTO measurements (location_id, sensors_id, datetime, value)
                VALUES %s
                ''',
                records
            )

        conn.commit()



