import os
from dotenv import load_dotenv
import requests
import psycopg2
import logging
import threading
import time

load_dotenv()

# Configure logging settings
logging.basicConfig(
    filename='fetch_and_insert.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Log environment variables for debugging
logging.debug("DB_NAME: %s", os.getenv('DB_NAME'))
logging.debug("DB_USER: %s", os.getenv('DB_USER'))
logging.debug("DB_PASSWORD: %s", os.getenv('DB_PASSWORD'))
logging.debug("DB_HOST: %s", os.getenv('DB_HOST'))
logging.debug("DB_PORT: %s", os.getenv('DB_PORT'))
logging.debug("API_BASE_URL: %s", os.getenv('API_BASE_URL'))
logging.debug("BEARER_TOKEN: %s", os.getenv('BEARER_TOKEN'))
logging.debug("API_PRODUCTS_ENDPOINT: %s", os.getenv('API_PRODUCTS_ENDPOINT'))
logging.debug("API_OFFERS_ENDPOINT: %s", os.getenv('API_OFFERS_ENDPOINT'))
logging.debug("API_ORDERS_ENDPOINT: %s", os.getenv('API_ORDERS_ENDPOINT'))
logging.debug("ORG_ID: %s", os.getenv('ORG_ID'))

def fetch_and_sync_data(data_type):
    base_urls = {
    'products': f"{os.getenv('API_BASE_URL')}{os.getenv('API_PRODUCTS_ENDPOINT')}",
    'offers': f"{os.getenv('API_BASE_URL')}{os.getenv('API_OFFERS_ENDPOINT')}",
    'orders': f"{os.getenv('API_BASE_URL')}{os.getenv('API_ORDERS_ENDPOINT')}"
}
    org_id = os.getenv('ORG_ID')
    page = 1
    headers = {'Authorization': f"Bearer {os.getenv('BEARER_TOKEN')}"}
    all_data_ids = set()

    # Construct the URL and fetch data
    while True:
        url = f"{base_urls[data_type]}?status=null&page={page}&org_id={org_id}"
        logging.debug(f"Requesting URL: {url}")
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(f"{data_type.capitalize()} data retrieved successfully for page {page}!")
            logging.debug(f"Response content: {response.content}")

            # Parse the response from json
            data = response.json().get('data', []) if isinstance(response.json(), dict) else response.json()
            if not data:
                logging.info(f"No more {data_type} found in the JSON response.")
                break

            # Add data IDs to the set
            all_data_ids.update(item.get('id') for item in data)

            # Database connection and data manipulation
            with psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            ) as conn:
                with conn.cursor() as cursor:
                    logging.info("Database connection established!")
                    # Insert or update data in the database
                    for item in data:
                        try:
                            if data_type == 'products':
                                cursor.execute('''
                                INSERT INTO products (id, user_id, org_id, product_name, quantity_type, sku, group_id, product_group, created_at, updated_at, status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO UPDATE SET
                                    user_id = EXCLUDED.user_id,
                                    org_id = EXCLUDED.org_id,
                                    product_name = EXCLUDED.product_name,
                                    quantity_type = EXCLUDED.quantity_type,
                                    sku = EXCLUDED.sku,
                                    group_id = EXCLUDED.group_id,
                                    product_group = EXCLUDED.product_group,
                                    created_at = EXCLUDED.created_at,
                                    updated_at = EXCLUDED.updated_at,
                                    status = EXCLUDED.status
                                ''', tuple(item.get(k) for k in [
                                    'id', 'user_id', 'org_id', 'product_name', 'quantity_type', 'sku',
                                    'group_id', 'product_group', 'created_at', 'updated_at', 'status'
                                ]))
                                logging.info(f"Inserted/Updated product: {item.get('id')}")
                            elif data_type == 'offers':
                                cursor.execute('''
                                INSERT INTO offers (id, request_id, project_id, offer_comments, offer_number, is_sent, is_received, is_active, created_at, updated_at, status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO UPDATE SET
                                    request_id = EXCLUDED.request_id,
                                    project_id = EXCLUDED.project_id,
                                    offer_comments = EXCLUDED.offer_comments,
                                    offer_number = EXCLUDED.offer_number,
                                    is_sent = EXCLUDED.is_sent,
                                    is_received = EXCLUDED.is_received,
                                    is_active = EXCLUDED.is_active,
                                    created_at = EXCLUDED.created_at,
                                    updated_at = EXCLUDED.updated_at,
                                    status = EXCLUDED.status
                                ''', tuple(item.get(k) for k in [
                                    'id', 'request_id', 'project_id', 'offer_comments', 'offer_number',
                                    'is_sent', 'is_received', 'is_active', 'created_at', 'updated_at', 'status'
                                ]))
                                logging.info(f"Inserted/Updated offer: {item.get('id')}")
                            elif data_type == 'orders':
                                cursor.execute('''
                                INSERT INTO orders (id, contact_id, offer_id, incoterms, offer_number, currency, total_price, total_tax, total_discount, company_name, status, created_at, request_name, project_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO UPDATE SET
                                    contact_id = EXCLUDED.contact_id,
                                    offer_id = EXCLUDED.offer_id,
                                    incoterms = EXCLUDED.incoterms,
                                    offer_number = EXCLUDED.offer_number,
                                    currency = EXCLUDED.currency,
                                    total_price = EXCLUDED.total_price,
                                    total_tax = EXCLUDED.total_tax,
                                    total_discount = EXCLUDED.total_discount,
                                    company_name = EXCLUDED.company_name,
                                    status = EXCLUDED.status,
                                    created_at = EXCLUDED.created_at,
                                    request_name = EXCLUDED.request_name,
                                    project_id = EXCLUDED.project_id
                                ''', tuple(item.get(k) for k in [
                                    'id', 'contact_id', 'offer_id', 'incoterms', 'offer_number', 'currency',
                                    'total_price', 'total_tax', 'total_discount', 'company_name', 'status',
                                    'created_at', 'request_name', 'project_id'
                                ]))
                                logging.info(f"Inserted/Updated order: {item.get('id')}")
                        except Exception as e:
                            logging.error(f"Error processing {data_type}: {e}")
                    # Commit changes
                    conn.commit()

                    # Delete records that are no longer present
                    cursor.execute(f"SELECT id FROM {data_type}")
                    existing_ids = set(row[0] for row in cursor.fetchall())
                    ids_to_delete = existing_ids - all_data_ids
                    for id_to_delete in ids_to_delete:
                        cursor.execute(f"DELETE FROM {data_type} WHERE id = %s", (id_to_delete,))
                        logging.info(f"Deleted {data_type[:-1]}: {id_to_delete}")
                    conn.commit()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to retrieve {data_type} data: {e}")
            break

        page += 1

def periodic_task(interval, data_types):
    # Function to fetch data periodically
    def task():
        while True:
            for data_type in data_types:
                fetch_and_sync_data(data_type)
            time.sleep(interval)
    thread = threading.Thread(target=task)
    thread.daemon = True
    thread.start()

periodic_task(5, ['products', 'offers', 'orders'])  # Adjust interval as needed

# Main loop to keep the script running
while True:
    time.sleep(1)
