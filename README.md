# OpenAQ Data Migration Project

This project downloads air quality measurement data from the OpenAQ archive, saves monthly CSV files for selected monitoring locations, and populates a PostgreSQL database with location, sensor, and measurement data.

The current workflow is built around Helsinki air quality stations and January 2024 data.

## Features

- Fetches a city bounding box using OpenStreetMap Nominatim.
- Uses the OpenAQ API to find measurement locations inside that bounding box.
- Downloads daily compressed CSV files from the OpenAQ archive.
- Merges daily files into one monthly CSV per location.
- Stores generated CSV files in a local `data/` folder.
- Populates PostgreSQL tables for:
  - `locations`
  - `sensors`
  - `measurements`
- Uses `.env` variables for API and database configuration.
- Provides a simple command-line menu for running the main actions.

## Tech Stack

- Python
- PostgreSQL
- pandas
- psycopg2
- requests
- python-dotenv
- NumPy


