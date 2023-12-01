# Data Engineering Exercise

## Objective:
Design a simplified version of a data pipeline using a real-world scenario. The exercise focuses on your ability to integrate data from an external API, process it, and present insights in a manner that's valuable for product and analytics teams.

## Scenario:
 You are a part of a small online book company and your company wants to expand on the books provided by their online store. Your task if you choose to accept it, is to develop a mini data pipeline that extracts data from [Open Library's API](https://openlibrary.org/developers/api), focusing on a [subject](https://openlibrary.org/dev/docs/api/subjects) of your choice.

This pipeline will be generalized for production use at a later date as the company wishes to perform similar analysis in the future. The data to be extracted includes a list of authors and books related to your chosen subject.

## Key Deliverables

   * Note: for the locally run files both the "requests" and "pandas" packages are needed via pip install  

1. **Architecture Overview**:
   For this scenario I chose an AWS influenced architecture that takes scalability into account. At a high level, I used a lambda function which could be triggered
   by a variey of sources such as API Gateway, AWS SQS, AWS SNS, etc. to pull data from the Openlibrary API (I used space as my subject) which then splits the data into two different partitoned directories in AWS S3. One bucket was created for authors, another bucket was created for books, and both are partitioned by first publish year. Once the files are in S3, AWS Glue would crawl and catalog the data, which would then be queried using AWS Athena in order to perform aggregations and output those aggregations back into S3. I included an option to write the API data to RDS as well which could potentially be better for light data loads that require more complex querying.

   Here is a link to my google drive which has a diagram of the data flow and architecture: https://drive.google.com/file/d/10d8YgfiLXsT__IseQcrWybgfZwG60pPg/view?usp=sharing

2. **Python Script**:
   All relevant scripts can be found in the exercise_scripts directory. There you'll find four python scripts that contain the logic for the lambda function data extracts, one for S3 and another for RDS, as well as a script that can be run locally to see the output of the S3 lambda and finally a script for the pythonic version of the AWS Athena aggregation queries.

3. **Sample Data Output**:
   Sample data can be found in the authors and book directories which are partitioned by first publish year, the same way that they would appear in an S3 bucket.

4. **Data Processing**:
   The data aggregation queries can be found in the athena_queries.sql file and the pythonic version of these queries can be found in the data_aggregation.py file which can be run locally. There are two outputs, average_books_per_year.csv and books_per_year.csv

5. **Presentation**:
    Here is a link to my presentation which covers the architecture and tools approach as well as potential enhancements: https://docs.google.com/presentation/d/1vWKQeztxXW9Tqirya6d_3dq5jnCsVex0GLxtEWM7wAY/edit?usp=sharing