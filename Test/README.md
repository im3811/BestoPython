
# README

## Overview

This script is designed to fetch data from a specified API and sync it with a PostgreSQL database. It periodically updates three types of data: products, offers, and orders. The data retrieval and database insertion operations are logged for debugging and monitoring purposes.

## Prerequisites

Before running the script, ensure you have the following:

1. **Environment Variables**: The script relies on environment variables for configuration. These variables should be defined in a `.env` file:
   - `DB_NAME`: The name of the PostgreSQL database.
   - `DB_USER`: The database user.
   - `DB_PASSWORD`: The database password.
   - `DB_HOST`: The database host.
   - `DB_PORT`: The database port.
   - `API_BASE_URL`: The base URL of the API.
   - `BEARER_TOKEN`: The bearer token for API authentication.

2. **Dependencies**: Install the required Python packages:
   - `requests`
   - `psycopg2`
   - `python-dotenv`

## Configuration

1. **Logging**: The script logs activities to `fetch_and_insert.log`. The logging level is set to `DEBUG` to capture detailed information.

2. **Environment Variables**: Load the environment variables from a `.env` file using the `dotenv` package.

## Functionality

### Data Fetching

The `fetch_and_sync_data` function retrieves data from the API for a specified data type (`products`, `offers`, or `orders`). It constructs the API request URL and handles pagination to fetch all available data.

### Database Operations

The script connects to the PostgreSQL database using the credentials provided in the environment variables. It then inserts or updates the fetched data into the corresponding database tables (`products`, `offers`, `orders`). If there are any existing records that are no longer present in the fetched data, they are deleted from the database.

### Periodic Data Sync

The `periodic_task` function sets up a periodic task to fetch and sync data at a specified interval. In this script, the interval is set to 5 seconds, and it processes `products`, `offers`, and `orders`.

## Execution

To run the script, simply execute it in a Python environment. It will continuously fetch and sync data based on the specified interval and data types.

```sh
python script.py
```

The script will run indefinitely, periodically updating the database with the latest data from the API. Logs will be generated in `fetch_and_insert.log` for monitoring and troubleshooting.
