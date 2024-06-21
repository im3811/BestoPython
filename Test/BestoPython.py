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

# Log database and API details
logging.debug("DB_NAME: %s", os.getenv('DB_NAME'))
logging.debug("DB_USER: %s", os.getenv('DB_USER'))
logging.debug("DB_PASSWORD: %s", os.getenv('DB_PASSWORD'))
logging.debug("DB_HOST: %s", os.getenv('DB_HOST'))
logging.debug("DB_PORT: %s", os.getenv('DB_PORT'))
logging.debug("API_BASE_URL: %s", os.getenv('API_BASE_URL'))
logging.debug("BEARER_TOKEN: %s", os.getenv('BEARER_TOKEN'))

def fetch_and_sync_data(data_type):
    base_urls = {
        'products': f"{os.getenv('API_BASE_URL')}/api/get/products/ea6c09e9-f387-4e10-ae58-019708e3fc16",
        'offers': f"{os.getenv('API_BASE_URL')}/api/get/offers/ea6c09e9-f387-4e10-ae58-019708e3fc16",
        'orders': f"{os.getenv('API_BASE_URL')}/api/get/orders/ea6c09e9-f387-4e10-ae58-019708e3fc16"
    }
    org_id = '51412ef9-a614-4116-85e6-3ac0be8cd6f5'
    page = 1

    headers = {
        'Authorization': f"Bearer {os.getenv('BEARER_TOKEN')}"
    }

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
            if isinstance(response.json(), list):
                data = response.json()
            else:
                data = response.json().get('data', [])
            
            if not data:
                logging.info(f"No more {data_type} found in the JSON response.")
                break
            
            # Add data IDs to the set 
            all_data_ids.update(item.get('id') for item in data)
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to retrieve {data_type} data: {e}")
            break

        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        
        try:
            conn = psycopg2.connect(
                dbname=db_name, 
                user=db_user, 
                password=db_password, 
                host=db_host, 
                port=db_port
            )
            logging.info("Database connection established!")
            
            cursor = conn.cursor()
            # Insert queries
            for item in data:
                try:
                    if data_type == 'products':
                        # Query for products remains unchanged
                        pass
                    elif data_type == 'offers':
                        # Removed user_id from offers
                        id = item.get('id')
                        org_id = item.get('org_id')
                        offer_comments = item.get('offer_comments')
                        offer_number = item.get('offer_number')
                        is_sent = item.get('is_sent')
                        is_received = item.get('is_received')
                        is_active = item.get('is_active')
                        created_at = item.get('created_at')
                        updated_at = item.get('updated_at')
                        status = item.get('status')
                        
                        insert_query = '''
                        INSERT INTO offers (id, org_id, offer_comments, offer_number, is_sent, is_received, is_active, created_at, updated_at, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                        SET org_id = EXCLUDED.org_id,
                            offer_comments = EXCLUDED.offer_comments,
                            offer_number = EXCLUDED.offer_number,
                            is_sent = EXCLUDED.is_sent,
                            is_received = EXCLUDED.is_received,
                            is_active = EXCLUDED.is_active,
                            created_at = EXCLUDED.created_at,
                            updated_at = EXCLUDED.updated_at,
                            status = EXCLUDED.status;
                        '''
                        cursor.execute(insert_query, (id, org_id, offer_comments, offer_number, is_sent, is_received, is_active, created_at, updated_at, status))
                        logging.info(f"Inserted/Updated offer: {id}")
                    elif data_type == 'orders':
                        # Removed user_id from orders
                        pass
                        
                except Exception as e:
                    logging.error(f"Error inserting {data_type} data: {e}")
            
            conn.commit()
            logging.info(f"{data_type.capitalize()} data inserted/updated successfully for page {page}!")

            cursor.execute(f"SELECT id FROM {data_type}")
            existing_ids = set(row[0] for row in cursor.fetchall())

            ids_to_delete = existing_ids - all_data_ids
            for id_to_delete in ids_to_delete:
                delete_query = f"DELETE FROM {data_type} WHERE id = %s"
                cursor.execute(delete_query, (id_to_delete,))
                logging.info(f"Deleted {data_type[:-1]}: {id_to_delete}")
            
            conn.commit()
            
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
        
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


periodic_task(5, ['products', 'offers', 'orders'])

while True:
    time.sleep(1)
