import logging
from flask import Flask, request
from google.cloud import bigquery
from datetime import datetime, timedelta

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

@app.route('/', methods=['POST'])
def insert_timestamp():
    client = bigquery.Client()

    # Define the dataset and table
    table_id = 'pratik9aug2024.10aug2024pratikdataset.RECON'

    # Get the current time in UTC and convert it to IST
    utc_now = datetime.utcnow()
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset

    # Convert datetime to ISO format string
    ist_now_str = ist_now.isoformat()

    logging.info(f"Inserting timestamp: {ist_now_str}")

    # Prepare the data to insert
    rows_to_insert = [
        {u'field1': ist_now_str}  # Insert the current IST timestamp as a string
    ]

    # Insert the data
    errors = client.insert_rows_json(table_id, rows_to_insert)
    
    if errors == []:
        logging.info("Inserted IST timestamp successfully.")
        return 'Inserted IST timestamp successfully.', 200
    else:
        logging.error(f"Encountered errors while inserting: {errors}")
        return f'Encountered errors while inserting: {errors}', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

