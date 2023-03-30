import time
import json
import random
from sqlalchemy import create_engine
from typing import Dict, List, Optional

# Define constants for connecting to DB
HOSTNAME = 'postgres'
DB_USER = 'veryfi'
DB_PASSWORD = 'veryfi'
DB_NAME = 'veryfidev'
DB_PORT = '5432'

# Define the databse engine: create_engine('postgresql://user:password@hostname/database_name')
DB_ENGINE = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{HOSTNAME}:{DB_PORT}/{DB_NAME}")

# Queries to create tables (if they don't exist)
QUERY_CREATE_DOCUMENTS_TABLE = '''
CREATE TABLE IF NOT EXISTS documents (
    document_id SERIAL PRIMARY KEY,
    ml_response VARCHAR
);
'''
QUERY_CREATE_BOOKMARK_TABLE = '''
CREATE TABLE IF NOT EXISTS bookmark (
    batch_size INT 
);
'''
QUERY_CREATE_ANALYTICS_TABLE = '''
CREATE TABLE IF NOT EXISTS parsed_total (
    id SERIAL PRIMARY KEY,
    business_id INTEGER,
    num_total INTEGER,
    total_value NUMERIC(10,2),
    total_ocr_score NUMERIC(10,2),
    total_ai_score NUMERIC(10,2),
    time_stamp TIMESTAMP
    );
'''


def initialize_tables() -> None:
    """
    Creates the following tables if they are not present
    "documents": for storing the scanned receipts of different busniness
    "bookmark": for keeping the record of the last entry analyzed
    "parse_table": for updating the records of transactions after a fixed time interval
    """
    try:
        with DB_ENGINE.connect() as conn:
            conn.execute(QUERY_CREATE_DOCUMENTS_TABLE)
            conn.execute(QUERY_CREATE_BOOKMARK_TABLE)
            conn.execute(QUERY_CREATE_ANALYTICS_TABLE)
            conn.execute("INSERT INTO bookmark VALUES(0);")
        print("Initialized the tables")
    except Exception as e:
        print(f"Error creating the tables: {e}")


def generate_bounding_box() -> List:
    """
    Generates a bounding box for a value scannned from a receipt. A bounding box in essence,
    is a rectangle that surrounds an object. Bounding boxes are mainly used in the task of
    object detection, where the aim is identifying the position. Bounding boxes can be specified as

    [x1, y1, x2, y2]

    - (x1, y1): x and y coordinate of the bottom-left corner of the bounding box,
    - (x2, y2): x and y coordinate of the top-right corner of the bounding box.

    For the bounding box to be valid x1 < x2 and y1 < y2.

    Returns:
        A list specifyin the bounding box
    """
    x1, x2 = 1.0, 0.0
    y1, y2 = 1.0, 0.0

    # Logic to generate a bounding box of finite area
    while x1 >= x2 or y1 >= y2:
        x1, y1, x2, y2 = (round(random.random(), 2) for i in range(4))

    return [x1, y1, x2, y2]


def rand_paylod() -> Optional[Dict]:
    """
    Generates a random payload containing the details of the bounding box.
    This payload is written to the database.
     
    Returns:
       A dictionary contianing payload has the following fields:
        - value: the actual value for this given field
        - score: how confident the model is for this above value for this given field
        - ocr_score: how confidence the OCR is for this above value for this given field
        - bounding_boxes: list of 4 vertices that form a bounding box for this given field in the document
    """
    payload = {}
    if random.randint(0, 10) > 5:
        payload = {
            "value" : random.randint(0, 1000),
            "score" : round(random.random(), 2),
            "ocr_score" : round(random.random(), 2),
            "bounding_box" : generate_bounding_box()
        }
        return payload


def add_transaction(payload: Dict) -> int:
    """
    Adds a row to the "documents" table based on the payload.

    Args:
        payload: A dictionary containing the bounding boxes of scanned receipt transactions coming from
                different businesses. For instance, see below

                {'business_id': 1,
                 'total': None,
                 'line_items': [
                    None, {'value': 179, 'score': 0.8, 'ocr_score': 0.6, 'bounding_box': [0.6, 0.3, 0.95, 0.96]},
                    {'value': 254, 'score': 0.25, 'ocr_score': 0.18, 'bounding_box': [0.14, 0.5, 0.24, 0.89]},
                    None, {'value': 97, 'score': 0.01, 'ocr_score': 0.71, 'bounding_box': [0.37, 0.58, 0.61, 0.89]},
                    None, None, {'value': 399, 'score': 0.85, 'ocr_score': 0.94, 'bounding_box': [0.84, 0.61, 0.87, 0.84]},
                    None
                    ]
                }
    Return:
        int: 0 if successfully written to db, else 1
    """
    try:
        query = f"INSERT INTO documents (ml_response) VALUES ('{json.dumps(payload)}');"
        with DB_ENGINE.connect() as conn:
            conn.execute(query)
        return 0
    except Exception as e:
        print(f"Cannot write to DB: {e} \n Attempt again")
        return 1

time.sleep(5)
initialize_tables()
print("Connection to DB established. Initiating writing data to the 'documents' table..\n")

# The logic below simulates a continuously running job that writes a new row to
# a database. Once 24000 rows have been writte, one row is written every 3 second
entries = 0
while True:
    choices = [
        rand_paylod(),
        [rand_paylod() for i in range(0, 9)]
    ]
    payload = {"business_id" : random.randint(0, 9)}
    fields = ["total", "line_items"]
    
    for f in fields:
        payload[f] = choices[random.randint(0, 9) % len(fields)]
        
    entries += 1 - add_transaction(payload)

    if entries > 24000:
        entries = 24000
        time.sleep(3)
