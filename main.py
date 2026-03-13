from methods import * 

#=======================================================================================#
# Yleistä infoa tehtävän suorituksesta:
# Tehtävän suorittamiseen on käytetty tuntien materiaaleja hyödyksi, mutta myös tekoälyä.
# Tekoälyn käytön painopiste oli lähinnä koodin ongelmien etsimisessä, sekä databasen pyörityksessä (databasemodelin rakennus, siihen perehtyminen ja ymmärrys)
# Tekoälyn käytössä promptit eivät ikinä olleen "Tee minulle tämä" -tyylisiä, vaan "Auta minua löytämään vika" tai "Selitä minulle asian x toiminta"
# Lopputulemana ymmärrän siis myös itse mitä koodissa tapahtuu.  
#=======================================================================================#

def run():
    while True:
        _choice = input(
            "Choose action:  "
            "\n(0: Exit, "
            "\n1: Populate locations -table"
            "\n2: Populate sensors -table"
            "\n3: Populate measurements -table"
            "\n4: Create dataset from API and put under folder 'data' "
        )

        if _choice == "0":
            break
        elif _choice == "1":
            populate_locations()
        elif _choice == "2":
            populate_sensors()
        elif _choice == "3":
            populate_measurements()
        elif _choice == "4":
            bbox = get_bbox("Helsinki") #ENSIMMÄINEN FUNKTIO
            print(bbox)
            locations = get_openaq_locations_by_bbox(bbox) #TOINEN FUNKTIO
            print("Found locations:", len(locations))

            for loc in locations: #alempana response body, jonka mukaan nämä tehty
                location_id = loc["id"]
                location_name = loc["name"]
                country_name = loc["country"]["name"]
                city_name = loc.get("locality")

                print("Location:", location_id, location_name)
                print("City:", city_name)
                print("Country:", country_name)
                #download_file_by_location(location_id, 2024, 1, 1) Tämä antaa yhden päivän tiedot
                download_and_merge_month(location_id,2024,1,city_name,country_name) #KOLMAS FUNKTIO
                #Tämä antaa yhden location_id:n tiedot yhdeltä kuukaudelta
    print()
    print("done")

if __name__ == "__main__":
    run()

#=======================================================================================#
#                 MUISTIINPANOJA TEHTÄVÄN TUEKSI
#=======================================================================================#
"""
==========RESPONSE BODY OPENAQ================
  "results": [
    {
      "id": 2975,
      "name": "Vartiokylä Huivipolku",
      "locality": "Helsinki",
      "timezone": "Europe/Helsinki",
      "country": {
        "id": 55,
        "code": "FI",
        "name": "Finland"
      },
==========RESPONSE BODY OPENAQ================
"""
"""      
Found locations: 6  
Location: 2975 Vartiokylä Huivipolku
Location: 2998 Leppävaara 4
Location: 4529 Tikkurila 3
Location: 4588 Mannerheimintie
Location: 4593 Kallio 2
Location: 9287 Mäkelänkatu
"""
"""
CSV-file:
location_id --> 2975, 2998, 4529, 4588, 4593, 9287
sensors_id --> PM2.5 = 27740, PM10 = 2002989, 03 = 6403, NO2 = 6306
location --> Asemakoodi + OpenAQ id: FI00841-2998, FI00370-4529, FI00564-4588, FI00425-4593, FI00902-9287
datetime --> aika
lat --> latitudi (sama per location_id)
lon --> longitudi (sama per location_id)
parameter -->PM2.5 (pienet partikkelit), PM10(isot partikkelit), O3 (otsoni), NO2 (typpidioksidi), 
units --> µg/m³
value --> lukema
"""