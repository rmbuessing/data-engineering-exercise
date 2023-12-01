# This file is basically a copy of the data_extract_lambda_s3.py file but it writes directly to RDS instead of S3
# This could be better for real-time data that can handle more complex queries at the cost of scalability

import json
import requests
import psycopg2
import os

def connect_to_db():
    # Database connection parameters - these should be securely stored in something like AWS Secrets Manager and referenced as env variables
    # This would be an AWS RDS database, the RDS instance 
    host = os.getenv('DB_HOST')
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    port = os.getenv('DB_PORT', 5432)

    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )
    return conn

def insert_data(cursor, table, data):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    # Handle duplicate rows with ON CONFLIUCT DO NOTHING
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    cursor.execute(query, list(data.values()))


def lambda_handler(event, context):
    # Fetch data from Open Library API
    response = requests.get('https://openlibrary.org/subjects/space.json?details=false')
    data = response.json()

    # Connect to the database
    conn = connect_to_db()
    cursor = conn.cursor()

    try:
        # Iter over items in works, first for authors then book data, and insert into db
        for work in data['works']:
            for author in work['authors']:
                # Dict for author data
                author_data = {
                    'author_id': author['key'],
                    'name': author['name']
                }
                insert_data(cursor, 'Authors', author_data)
            # Dict for book data
            book_data = {
                'book_id': work['key'],
                'title': work['title'],
                'first_publish_year': work['first_publish_year'],
                'cover_id': work.get('cover_id'),
                'cover_edition_key': work.get('cover_edition_key'),
                'author_id': work['authors'][0]['key'] if work['authors'] else None
            }
            insert_data(cursor, 'Books', book_data)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        # Close the database connection
        cursor.close()
        conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed successfully')
    }

# Invocation
lambda_handler(None, None)