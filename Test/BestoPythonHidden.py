import os
from dotenv import load_dotenv
import requests
import psycopg2
import json
import logging
import threading
import time

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='fetch_and_insert.log',  # Log to a file
    level=logging.DEBUG,  # Set the log level to DEBUG for detailed logging
    format='%(asctime)s - %(levelname)s - %(message)s'  # Set the log format
)

# Print environment variables to log for debugging
logging.debug("DB_NAME: %s", os.getenv('DB_NAME'))
logging.debug("DB_USER: %s", os.getenv('DB_USER'))
logging.debug("DB_PASSWORD: %s", os.getenv('DB_PASSWORD'))
logging.debug("DB_HOST: %s", os.getenv('DB_HOST'))
logging.debug("DB_PORT: %s", os.getenv('DB_PORT'))
logging.debug("API_BASE_URL: %s", os.getenv('API_BASE_URL'))
logging.debug("BEARER_TOKEN: %s", os.getenv('BEARER_TOKEN'))

def fetch_and_insert_data(data_type):
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

    while True:
        url = f"{base_urls[data_type]}?status=null&page={page}&org_id={org_id}"
        logging.debug(f"Requesting URL: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(f"{data_type.capitalize()} data retrieved successfully for page {page}!")
            logging.debug(f"Response content: {response.content}")
            
            if isinstance(response.json(), list):
                data = response.json()
            else:
                data = response.json().get('data', [])
            
            if not data:
                logging.info(f"No more {data_type} found in the JSON response.")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to retrieve {data_type} data: {e}")
            break

        # Database connection
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
            
            for item in data:
                try:
                    if data_type == 'products':
                        # Extracting product details
                        id = item.get('id')
                        user_id = item.get('user_id')
                        org_id = item.get('org_id')
                        product_name = item.get('product_name')
                        quantity_type = item.get('quantity_type')
                        sku = item.get('sku')
                        group_id = item.get('group_id')
                        product_group = item.get('product_group')
                        created_at = item.get('created_at')
                        updated_at = item.get('updated_at')
                        status = item.get('status')
                        
                        # Inserting data into the products table
                        insert_query = '''
                        INSERT INTO products (id, user_id, org_id, product_name, quantity_type, sku, group_id, product_group, created_at, updated_at, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        '''
                        cursor.execute(insert_query, (id, user_id, org_id, product_name, quantity_type, sku, group_id, product_group, created_at, updated_at, status))
                        logging.info(f"Inserted product: {id}")
                    elif data_type == 'offers':
                        # Extracting offer details
                        id = item.get('id')
                        request_id = item.get('request_id')
                        project_id = item.get('project_id')
                        offer_comments = item.get('offer_comments')
                        offer_number = item.get('offer_number')
                        is_sent = item.get('is_sent')
                        is_received = item.get('is_received')
                        is_active = item.get('is_active')
                        created_at = item.get('created_at')
                        updated_at = item.get('updated_at')
                        status = item.get('status')
                        
                        # Inserting data into the offers table
                        insert_query = '''
                        INSERT INTO offers (id, request_id, project_id, offer_comments, offer_number, is_sent, is_received, is_active, created_at, updated_at, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        '''
                        cursor.execute(insert_query, (id, request_id, project_id, offer_comments, offer_number, is_sent, is_received, is_active, created_at, updated_at, status))
                        logging.info(f"Inserted offer: {id}")
                    elif data_type == 'orders':
                        # Extracting order details
                        id = item.get('id')
                        contact_id = item.get('contact_id')
                        offer_id = item.get('offer_id')
                        incoterms = item.get('incoterms')
                        offer_number = item.get('offer_number')
                        currency = item.get('currency')
                        total_price = item.get('total_price', 0.0)
                        total_tax = item.get('total_tax', 0.0)
                        total_discount = item.get('total_discount', 0.0)
                        company_name = item.get('company_name')
                        status = item.get('status')
                        created_at = item.get('created_at')
                        request_name = item.get('request_name')
                        project_id = item.get('project_id')
                        
                        # Inserting data into the orders table
                        insert_query = '''
                        INSERT INTO orders (id, contact_id, offer_id, incoterms, offer_number, currency, total_price, total_tax, total_discount, company_name, status, created_at, request_name, project_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        '''
                        cursor.execute(insert_query, (id, contact_id, offer_id, incoterms, offer_number, currency, total_price, total_tax, total_discount, company_name, status, created_at, request_name, project_id))
                        logging.info(f"Inserted order: {id}")
                except Exception as e:
                    logging.error(f"Error inserting {data_type} data: {e}")
            
            conn.commit()
            logging.info(f"{data_type.capitalize()} data inserted successfully for page {page}!")
            
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
        
        page += 1

def periodic_task(interval, data_types):
    def task():
        while True:
            for data_type in data_types:
                fetch_and_insert_data(data_type)
            time.sleep(interval)
    thread = threading.Thread(target=task)
    thread.daemon = True
    thread.start()

# Start periodic task with a 30-second interval
periodic_task(30, ['products', 'offers', 'orders'])

# Keep the main program running
while True:
    time.sleep(1)
