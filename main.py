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
        )

        if _choice == "0":
            break
        elif _choice == "1":
            populate_locations()
        elif _choice == "2":
            populate_sensors()
        elif _choice == "3":
            populate_measurements()
 
    print()
    print("done")

if __name__ == "__main__":
    run()