# This is a lambda function that extracts data from the openlibrary api, separates the data into two csv files,
# authors.csv and books.csv, and does a mock write to an S3 bucket. This is better for heavy data loads as S3 is 
# inherently more scalable than RDS instances (RDS instances need to be scaled depending on data size). Lastly the 
# files are partitioned by the first_publish_year field. For authors, this makes the assumption that an authors earliest work 
# is indicative of their first appearance.

import json
import requests
import csv
import boto3
import os
from io import StringIO

# Initialize S3 client
s3_client = boto3.client('s3')

def upload_to_s3(bucket, path, content):
    s3_client.put_object(Bucket=bucket, Key=path, Body=content)

def lambda_handler(event, context):
    # Fetch data from Open Library API
    response = requests.get('https://openlibrary.org/subjects/space.json?details=false')
    data = response.json()

    # AWS S3 bucket details
    bucket_name = os.getenv('s3_bucket_name')

    authors_data = {}
    books_data = {}

    for work in data['works']:
        year = work.get('first_publish_year')
        for author in work['authors']:
            # Initialize the year for each author if not already set
            if author['key'] not in authors_data or (year and year < authors_data[author['key']]['year']):
                authors_data[author['key']] = {'year': year, 'name': author['name']}

        # Process books data
        if year not in books_data:
            books_data[year] = StringIO()
        csv_writer = csv.writer(books_data[year])
        if books_data[year].tell() == 0:
            csv_writer.writerow(['book_id', 'title', 'first_publish_year', 'cover_id', 'cover_edition_key', 'author_id'])
        author_id = work['authors'][0]['key'] if work['authors'] else None
        csv_writer.writerow([
            work['key'], 
            work['title'], 
            year, 
            work.get('cover_id'), 
            work.get('cover_edition_key'), 
            author_id
        ])

    # Upload authors data partitioned by first known publication year
    for author_key, author_info in authors_data.items():
        year = author_info['year'] if author_info['year'] else 'unknown'
        path = f'authors/year={year}/{author_key}.csv'
        content = StringIO()
        csv_writer = csv.writer(content)
        csv_writer.writerow(['author_id', 'name'])
        csv_writer.writerow([author_key, author_info['name']])
        upload_to_s3(bucket_name, path, content.getvalue())
        content.close()

    # Upload books data partitioned by first_publish_year
    for year, csv_content in books_data.items():
        if year:
            path = f'books/year={year}/books.csv'
        else:
            path = 'books/year=unknown/books.csv'
        upload_to_s3(bucket_name, path, csv_content.getvalue())
        csv_content.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed and uploaded to S3 successfully')
    }

# Invocation
lambda_handler(None, None)
