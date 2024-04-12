# Data Pipeline Engineering 


## Requirements -
1. Run pip install -r requirements.txt to install the dependencies(pandas, psycopg2)
2. Prepare the destination RDBMS and create a database for the data.
3. Modify db_connection.py file with your db credentials and the schema name

The source data have been clean, modified and merged using inner join and full outer join.
The columns of resultant Dataframe(variable name: "column_definitions") has been used to create a table.

### Ingestion of the Data files in this Repo (Transformations will also be done)
    1. Run python ingest.py
        -this file imports transformations.py which transforms and merges the data


### Data Ingestion and Schema Evolution Using Different datasets:

The data is ingested into the database using a variable name for the columns(column_names) and 
a placeholder variable. 
The ingestion is done in batches of 1000 so that the pipeline can handle large datasets.

IF there will be no transformations to the source data,
    1. Create a dataframe(df) with the name 'df' using pandas in the transform_ingest.ipynb file

    2. Run the code with the Markdown "TABLE CREATION: ..."" to create the DB table
        - The values of the variable, "column_definitions" will be used as the column names of
        the table.
    3. Run the code with the Markdown "INGESTION CODE:...." to insert the data into the DB
        -The column names and the values to use for the data ingestion are dynamic, there is no need for manually editing those fields.
        When the schema changes, the code will work without the need for any modification.


 **Fault Tolerance:** 
 At each stage, that is, DB table creation and data ingestion, if there is an error, the process
 stops and rolls back all transactions.




