# This is the pythonic version of the queries in the athena_queries.sql file that
# can be used to see some local sample outputs

import pandas as pd

# Read the CSV files
authors_df = pd.read_csv('authors.csv')
books_df = pd.read_csv('books.csv')

# Merge the authors and books dataframes on author_id
merged_df = pd.merge(books_df, authors_df, left_on='author_id', right_on='author_id')

# Aggregate data: Number of books written each year by an author
books_per_year = merged_df.groupby(['author_id', 'name', 'first_publish_year']).size().reset_index(name='total_books')

# Calculate average number of books written by an author per year
average_books_per_year = books_per_year.groupby(['author_id', 'name'])['total_books'].mean().reset_index(name='avg_books_per_year')

# Save the aggregated data to CSV files
books_per_year.to_csv('books_per_year.csv', index=False)
average_books_per_year.to_csv('average_books_per_year.csv', index=False)
