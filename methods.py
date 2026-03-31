import contextlib
import os
import psycopg2
import contextlib
import glob
import requests
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from urllib.parse import quote
import io
import numpy as np

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://openaq-data-archive.s3.amazonaws.com"

#=======================================================================================#
#                                  GET BBOX -FUNKTIO
#=======================================================================================#
def get_bbox(city):
    osm_url = f"https://nominatim.openstreetmap.org/search?q={quote(city)}&format=json"
    #city = Helsinki, tai mikä vaan
    headers = {'User-Agent': 'OpenAQCityBBox'}
    response = requests.get(osm_url, headers=headers).json()
    if not response:
        return None
    # boundingbox sisältää löydetyn kaupungin rajat
    # siinä on 4 koordinaattipisettä
    osm_bbox = response[0]['boundingbox']
    # OpenStreetMapin bounding boxin koordinaatit ovat ao järjestyksessä
    # min_y, max_y, min_x, max_x
    min_lat, max_lat, min_lon, max_lon = osm_bbox
    # järjestetään uudelleen openAQ:lle sopivaan muotoon: min_x, min_y, max_x, max_y
    openaq_bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    return openaq_bbox

#=======================================================================================#
#                          GET LOCATIONS BY USING BBOX -FUNKTIO
#=======================================================================================#
# tämä funktio saa parametrinaan kaupungin bounding boxin get_bbox-funktiolta
# locations-api ottaa yhtenä parametrina bbox:in, joten sitä käytetään tässä
# parametreja on myös muita, mutta bbox tässä tapauksesssa paras
def get_openaq_locations_by_bbox(_bbox):
    response = requests.get(
        f'https://api.openaq.org/v3/locations?limit=1000&page=1&order_by=id&sort_order=asc&bbox={_bbox}',
        headers={'X-API-Key': API_KEY})
    _locations = []
    if response.status_code == 200:
        _locations = response.json()['results']

    return _locations

#=======================================================================================#
#                     LATAA CSV-FILU SIJAINNIN MUKAAN -FUNKTIO
#=======================================================================================#

def download_file_by_location(location_id, year, month, day):
    date_str = f"{year}{month:02d}{day:02d}"
    key = f"records/csv.gz/locationid={location_id}/year={year}/month={month:02d}/location-{location_id}-{date_str}.csv.gz"
    full_url = f"{BASE_URL}/{key}"
  
    response = requests.get(full_url)
    if response.status_code == 200:
        # pandas osaa avata gzip-pakatun csv
        df = pd.read_csv(io.BytesIO(response.content), compression='gzip')
        df.to_csv(f"{location_id}-{date_str}.csv", index=False)
    else:
        print(f"Failed to fetch. Status: {response.status_code}")

#=======================================================================================#
#                 TEE CSV-FILU JOKAISELTA PÄIVÄLTÄ MITTAUSPAIKAN MUKAAN -FUNKTIO
#=======================================================================================#

def download_files_for_month(location_id, year, month):
    start_date = pd.Timestamp(year, month, 1) # Tää on joku pandasin hieno ominaisuus
    end_date = start_date + pd.offsets.MonthEnd(0)
    all_dates = pd.date_range(start=start_date, end=end_date)

    for date in all_dates:
        download_file_by_location(location_id, date.year, date.month, date.day) 
        # Käyttää vanhaa funktiota jolla ensin lataa per päivä kuukauden ajalta 
        # Tekee joka päivältä erilliset filut. 
        # We don't want that, kuukauden ajalta filuja tulee muuten aivan sairas määrä.

#=======================================================================================#
#                 KOKOA CSV-FILUT KUUKAUDEN AJALTA PER MITTAUSPAIKKA -FUNKTIO
#=======================================================================================#
def download_and_merge_month(location_id, year, month, city, country):
    start_date = pd.Timestamp(year, month, 1)
    end_date = start_date + pd.offsets.MonthEnd(0)
    all_dates = pd.date_range(start=start_date, end=end_date)

    df_list = []

    for date in all_dates:
        date_str = f"{date.year}{date.month:02d}{date.day:02d}"
        # tää on staattinen filestorage. Antaa raakadatan location_id:n mukaan
        key = f"records/csv.gz/locationid={location_id}/year={date.year}/month={date.month:02d}/location-{location_id}-{date_str}.csv.gz"
        full_url = f"{BASE_URL}/{key}"

        response = requests.get(full_url)

        if response.status_code == 200:
            df = pd.read_csv(io.BytesIO(response.content), compression='gzip')
            df_list.append(df)

    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        full_df["city"] = city
        full_df["country"] = country
        os.makedirs("data", exist_ok=True) #tehdään kansio
        # luotavat tiedostot on kuukauden ajalta jokaiselta bboxin sisällä olevalta mittauspisteeltä
        # mittauspisteitä kuusi, tunnistettavissa filun numen alussa olevan location_id:n mukaan. 
        filepath= f"data/{location_id}-{year}-{month:02d}-merged.csv" # filepath luotaville tiedostoille
        full_df.to_csv(filepath, index=False)
        return full_df
    else:
        print(f"No data found for location {location_id} in {year}-{month}")
        return None

@contextlib.contextmanager

#=======================================================================================#
#                 YHDISTÄ TIETOKANTAAN -FUNKTIO
#=======================================================================================#
# Haetaan tällä .env filusta tarvittavat muuttujat databaseen ja muuhun liittyen
def get_connection():
    conn = None
    try:
        conn = psycopg2.connect(
        dbname=os.getenv('DB'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PWD'),
        host="localhost",
        port=5432,
        options=os.getenv('DB_OPTIONS') # tällä määritelty databasen schema erikseen, ettei mene public.
)
        yield conn
    finally:
        if conn is not None:
            conn.close()

#=======================================================================================#
#                           DATABASEN TÄYTTÖFUNKTIOT 5 KPL
#=======================================================================================#
def populate_countries(folder_path="data"):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM city;") 
            cur.execute("DELETE FROM country;") 
            conn.commit()
            files = glob.glob(f"{folder_path}/*.csv")
            df_list = [pd.read_csv(file) for file in files]
            df = pd.concat(df_list, ignore_index=True)
  
            country_df = df[['country']].drop_duplicates()
            country_df = country_df.rename(columns={"country": "country_name"})

            records = [ 
                tuple(
                    x.item() if isinstance(x, (np.integer, np.floating)) else x
                    for x in row
                )
                for row in country_df.to_records(index=False)
            ]
            execute_values(
                cur,
                '''
                INSERT INTO country (country_name) 
                VALUES %s
                ''',
                records
            )
        conn.commit()

def populate_cities(folder_path="data"):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM locations;")
            cur.execute("DELETE FROM city;") 
            conn.commit()
            files = glob.glob(f"{folder_path}/*.csv")
            df_list = [pd.read_csv(file) for file in files]
            df = pd.concat(df_list, ignore_index=True)
  
            city_df = df[['country','city']].drop_duplicates()
            city_df = city_df.rename(columns={"city": "city_name"})

            cur.execute("SELECT country_id, country_name FROM country;")
            country_haku = {name: id for id, name in cur.fetchall()}

            city_df["country_id"] = city_df["country"].map(country_haku)
            city_df = city_df[['country_id', 'city_name']].drop_duplicates()

            records = [ 
                tuple(
                    x.item() if isinstance(x, (np.integer, np.floating)) else x
                    for x in row
                )
                for row in city_df.to_records(index=False)
            ]
            execute_values(
                cur,
                '''
                INSERT INTO city (country_id, city_name) 
                VALUES %s
                ''',
                records
            )
        conn.commit()

def populate_locations(folder_path="data"): # Osoitetaan kansio, missä filut on
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Poistetaan vanha data, jos sellaista on
            cur.execute("DELETE FROM measurements;") # Poistetaan ensin measurements, koska measurements käyttää locations_id foreign key:nä
            cur.execute("DELETE FROM locations;") # Vasta sitten poistetaan vanhat locations tablet
            conn.commit()

            # Luetaan CSV-filut
            files = glob.glob(f"{folder_path}/*.csv")
            df_list = [pd.read_csv(file) for file in files]
            df = pd.concat(df_list, ignore_index=True)

            # Valitaan halutut rivit, poistetaan duplikaatit
            locations_df = df[['location_id','lat','lon','location', 'city']].drop_duplicates()# drop_duplicates poistaa vain ne rivit, jotka ovat 100% identtisiä keskenään

            cur.execute("SELECT city_id, city_name FROM city;")
            city_haku = {name: id for id, name in cur.fetchall()}

            locations_df["city_id"] = locations_df["city"].map(city_haku)
            locations_df = locations_df[['location_id', 'lat', 'lon', 'location', 'city_id']]

            records = [
                tuple(
                    x.item() if isinstance(x, (np.integer, np.floating)) else x
                    for x in row
                )
                for row in locations_df.to_records(index=False)
            ]

            # Lisätään databaseen
            # Sama locations_id on eri CSV-filuissa useamman kerran, minkä vuoksi lisätty ehtolause ON CONFLICT
            execute_values(
                cur,
                '''
                INSERT INTO locations (location_id, lat, lon, location, city_id)
                VALUES %s
                ON CONFLICT (location_id) DO NOTHING
                ''',
                records
            )
        conn.commit()

def populate_sensors(folder_path="data"):
# Toistetaan sama kaava, kuin populate_locations()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM measurements;") # sensor_id foreign key:nä measurementseissa
            cur.execute("DELETE FROM sensors;") # vasta sitten poistetaan vanhat sensors tablet
            conn.commit()
            files = glob.glob(f"{folder_path}/*.csv")
            df_list = [pd.read_csv(file) for file in files]
            df = pd.concat(df_list, ignore_index=True)
  
            sensors_df = df[['sensors_id','parameter','units']].drop_duplicates() # drop_duplicates poistaa vain ne rivit, jotka ovat 100% identtisiä keskenään
            records = [ 
                tuple(
                    x.item() if isinstance(x, (np.integer, np.floating)) else x
                    for x in row
                )
                for row in sensors_df.to_records(index=False)
            ]
            # Sama homma: samaa sensorityyppiä käytetään useammassa lokaatiossa CSV-filujen välillä, joten lisättävä ehto ON CONFLICT
            execute_values(
                cur,
                '''
                INSERT INTO sensors (sensors_id, parameter, units) 
                VALUES %s
                ON CONFLICT (sensors_id) DO NOTHING
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

            records = [
                tuple(
                    x.item() if isinstance(x, (np.integer, np.floating)) else x
                    for x in row
                )
                for row in measurements_df.to_records(index=False)
            ]
            
            execute_values(
                cur,
                '''
                INSERT INTO measurements (location_id, sensors_id, datetime, value)
                VALUES %s
                ''',
                records
            )

        conn.commit()



