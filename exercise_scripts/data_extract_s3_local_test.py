# This file is essentially a copy of the data_extract_lambda_s3.py file minus the S3 operations,
# this can be used to see the CSV output of what would be stored in S3

import json
import requests
import csv
import os
from io import StringIO

def save_to_local(directory, filename, content):
    # Check and create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    # Full path for the file
    full_path = os.path.join(directory, filename)
    # Write content to the file
    with open(full_path, 'w', newline='', encoding='utf-8') as file:
        file.write(content)

def lambda_handler(event, context):
    response = requests.get('https://openlibrary.org/subjects/space.json?details=false')
    data = response.json()

    authors_data = {}
    books_data = {}

    for work in data['works']:
        year = work.get('first_publish_year')
        for author in work['authors']:
            if author['key'] not in authors_data or (year and (authors_data[author['key']]['year'] is None or year < authors_data[author['key']]['year'])):
                authors_data[author['key']] = {'year': year, 'name': author['name']}

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

    # Save authors data
    for author_key, author_info in authors_data.items():
        year = author_info['year'] if author_info['year'] else 'unknown'
        directory = 'authors' + os.sep + f'year={year}' 
        filename = 'authors.csv'
        content = StringIO()
        csv_writer = csv.writer(content)
        csv_writer.writerow(['author_id', 'name'])
        csv_writer.writerow([author_key, author_info['name']])
        content_str = content.getvalue()
        if content_str.strip():  # Check if there is content to write
            save_to_local(directory, filename, content_str)  # Pass the string content
            print(f"Saved: {directory + os.sep + filename}") # Debug print
        content.close()


    # Save books data
    for year, csv_content in books_data.items():
        year_str = str(year) if year else 'unknown'
        directory = 'books' + os.sep + f'year={year_str}'
        filename = 'books.csv'
        if csv_content.getvalue().strip():  # Check if there is content to write
            save_to_local(directory, filename, csv_content.getvalue())
            print(f"Saved: {directory + os.sep + filename}")  # Debug print
        csv_content.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed and saved to local partitioned CSV files successfully')
    }

lambda_handler(None, None)
