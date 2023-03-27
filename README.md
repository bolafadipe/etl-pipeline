# Data-Centralisation-project

Multinational Data Centralisation project, a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. Utilising the power of Pandas, the project  clean the data, and produce a STAR based database schema for optimised data storage and access. Builds complex SQL-based data queries, allowing  to extract valuable insights and make informed decisions. This project provided the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 

# Milestone 1
Developed a system that to extract and clean data.   
Three classes were initialised to manage the different acspects of data extraction and cleaning. 

Step 1: A new Python script named data_extraction.py and within it, create a class named DataExtractor. This class will work as a utility class, which contain  methods that help extract data. These methods extracts retail sales data from five different data sources; 
  PDF documents
  An AWS RDS database
  RESTful API
  JSON 
  CSV files

Step 2: Another script named database_utils.py and within it, created a class DatabaseConnector which  used to connect with and upload data to the database.

Step 3:Finally,  a script named data_cleaning.py this script  contain a class DataCleaning with methods to clean data from each of the data sources.

# Milsetone 2 Data Clean and uploading

Methods used to extract data from each resources are:
 1 An AWS RDS database - First, return the dictionary of RDS database credentials from a yaml file. Then, initialise and return an SQLAlchemy database engine using these credentials. Using the engines, data was extracted from the database table to a Pandas dataframe.
 2 PDF documents - Used tabula.read_pdf library that allows  to extract tables from PDF documents. It provides a simple interface for extracting tables from PDFs into Pandas DataFrame format.
 3 RESTful API -  Made the API request to get the response by giving headers and API endpoint.The API has two GET methods. One  returned the number of stores in the business and the other to retrieve the data of the store based on store number. The libraries imported were “requests” and “JSON”. The data returned in the form of text and stored into dataframe
  
  Amazon S3:  Boto3 is a Python library that provides a developer-friendly interface for interacting with Amazon Web Services (AWS). It is specifically designed for Python developers to write software that makes use of services like Amazon S3, Amazon EC2, and Amazon SQS. Once installed Boto3, it  extract data from an S3 bucket in the Python code. Data extracted from S3 was converted into Json format.


After all data was cleaned, it was uploaded into pg admin into 5 different tables and respective primary and foreign key was given to each table:

<img width="140" alt="image" src="https://user-images.githubusercontent.com/110827214/220201637-6cdbba18-8899-46f2-a245-63506b5be320.png">

# Milestone 3 Query the data

Once all data uploaded, database was ready to answer some business questions like:
 - which months produced most sales
 - what percentage of sales coming from each store type
 - how quickly is the company making sales
 

