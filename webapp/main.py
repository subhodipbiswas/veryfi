from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine
import requests
import pandas as pd
import json

# Define constants for connecting to DB
HOSTNAME = 'postgres'
DB_USER = 'veryfi'
DB_PASSWORD = 'veryfi'
DB_NAME = 'veryfidev'
DB_PORT = '5432'

# Define the constants
TABLE_NAME = "parsed_total"
NUM_ENTRIES = 5

# Connect to the database
DB_ENGINE = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{HOSTNAME}:{DB_PORT}/{DB_NAME}")

app = FastAPI()

@app.get("/")
async def index():
    """
    Hompage generated using FastAPI. Displays an image taken from online.
    """
    image_url = "https://smartengines.com/wp-content/uploads/2021/05/mobile_ocr.jpg"
    image_html = f"<img src='{image_url}'/>"
    return HTMLResponse(content=image_html)


@app.get("/api/business/{business_id}")
def get_total(business_id: int):
    """
    Generates information about the business in the last NUM_ENTRIES entries.

    Args:
        business_id: The ID of the business whose details we will show
    """
    try:
        query = """
        SELECT * 
        FROM {} 
        WHERE business_id = {}
        ORDER BY time_stamp DESC 
        LIMIT {};
        """.format(
            TABLE_NAME,
            business_id,
            NUM_ENTRIES
            )
        
        df = None
        with DB_ENGINE.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.all(), columns=result.keys())

        # Generate the values for display
        num_receipts = round(df.num_total.sum())
        
        statistics = {
            'Business ID #': business_id,
            '   Start Time': df['time_stamp'].iloc[-1],
            '     End Time': df['time_stamp'].iloc[0],
            ' No. Receipts': num_receipts,
            '   Total Sale': round(df.total_value.sum()),
            '     Avg Sale': round(df.total_value.sum()/num_receipts, 2),
            ' Avg AI Score': round(df.total_ai_score.sum()/num_receipts, 2),
            'Avg OCR Score': round(df.total_ocr_score.sum()/num_receipts, 2)
        }
        
        json_str = json.dumps(statistics, indent=10, default=str)
        return Response(content=json_str, media_type='application/json')
    except Exception as e:
        print(f"Exception occured for analyzing business {business_id}: ", e)
        return e
