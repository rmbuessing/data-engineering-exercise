-- Two tables with an author_id relationship that could be used for the RDS approach

CREATE TABLE Authors (
    author_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE Books (
    book_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    first_publish_year INT,
    cover_id INT,
    cover_edition_key VARCHAR(255),
    author_id VARCHAR(255),
    FOREIGN KEY (author_id) REFERENCES Authors(author_id)
);
