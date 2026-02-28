import pandas as pd
from methods import * 

# load_dotenv:n kutsu lataa arkaluontoiset tiedot ympäristömuuttujiin


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
            country_code = loc["country"]["code"]
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