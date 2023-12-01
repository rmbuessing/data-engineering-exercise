-- These are queries that could be used in AWS Athena for the two aggregations

-- Number of books written each year by an author
SELECT author_id, first_publish_year, COUNT(*) as total_books
FROM books
GROUP BY author_id, first_publish_year;

-- Average number of books written by author per year
SELECT author_id, AVG(total_books) as avg_books_per_year
FROM (
    SELECT author_id, first_publish_year, COUNT(*) as total_books
    FROM books
    GROUP BY author_id, first_publish_year
) as yearly_books
GROUP BY author_id;
