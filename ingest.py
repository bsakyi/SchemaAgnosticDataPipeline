import transformations
import db_connection
import re

con = db_connection.pgcon
cur = db_connection.pgcur
column_definitions = transformations.column_definitions 
df = transformations.df

########## TABLE CREATION CODE ##########
try:
    #  create table with column_definitions 
    create_table_query = f"CREATE TABLE IF NOT EXISTS CustomerSaleData ({column_definitions})"
    # print(create_table_query)
    
    # Execute the create table query
    cur.execute(create_table_query)
    
    # Commit the transaction
    con.commit()

except Exception as e:
    print("An error occurred:", str(e))
    # Rollback the transaction if an error occurs
    con.rollback()

# END OF TABLE CREATION CODE



###### BEGIN- DATA INGESTION CODE########
try:
    # Define the size of each batch
    batch_size = 1000

    # Insert DataFrame into PostgreSQL database in batches
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        values = [tuple(row) for row in batch_df.values]

        placeholders = ','.join(['%s'] * len(df.columns))
        # column_names = ','.join(df.columns)
        column_names = ', '.join([f"{re.sub('[^a-zA-Z]+', '', col.replace(' ', '_'))}" for col in df.columns])
        insert_query = f"INSERT INTO CustomerSaleData ({column_names}) VALUES ({placeholders})"
        print(insert_query)
        cur.executemany(insert_query, values)
        con.commit()

except Exception as e:
    print("An error occurred during batch data insertion:", e)
    con.rollback()  # Rollback the transaction in case of an error

# END - DATA INGESTION CODE

