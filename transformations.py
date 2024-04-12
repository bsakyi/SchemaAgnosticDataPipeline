# %%
import pandas as pd
import db_connection 
import re

# %%


# %%
df_cust1 = pd.read_json('Market 1 Customers.json')
df_delv1 = pd.read_csv('Market 1 Deliveries.csv')
df_ord1 = pd.read_csv('Market 1 Orders.csv')
df_cust2 = pd.read_json('Market 2 Customers.json')
df_delv2 = pd.read_csv('Market 2 Deliveries.csv')
df_ord2 = pd.read_csv('Market 2 Orders.csv')

# %%


# %%
# find completely empty columns
print("Market 1************")
empty_delv1_columns = df_delv1.columns[df_delv1.isna().all()].to_list()
print(f"Market 1 Delivery, Empty  Columns: {empty_delv1_columns}\n")

empty_cust1_columns = df_cust1.columns[df_cust1.isna().all()].to_list()
print(f"Market 1 Customers, Empty  Columns: {empty_cust1_columns}\n")

empty_ord1_columns = df_ord1.columns[df_ord1.isna().all()].to_list()
print(f"Market 1 Orders, Empty  Columns: {empty_ord1_columns}\n")

print("Market 2 ******************\n")

empty_delv2_columns = df_delv2.columns[df_delv2.isna().all()].to_list()
print(f"Market 2 Delivery, Empty  Columns: {empty_delv2_columns}\n")

empty_cust2_columns = df_cust2.columns[df_cust2.isna().all()].to_list()
print(f"Market 2 Customers, Empty  Columns: {empty_cust2_columns}\n")

empty_ord2_columns = df_ord2.columns[df_ord2.isna().all()].to_list()
print(f"Market 2 Orders, Empty  Columns: {empty_ord2_columns}\n")


# %%
# Delete all columns which have null values for all records
    # Market 1 Orders
df_ord1.dropna(axis=1, how='all', inplace=True)

    # Market 2 deliveries
df_delv2.dropna(axis=1, how='all', inplace=True)

    # Market 2 Orders
df_ord2.dropna(axis=1, how='all', inplace=True)

# %%
# rename Column
df_cust1.rename(columns={'Number of employees':'Number of Employees'}, inplace=True)

# %%


# %%
# strip off order_id to be same as the order id in orders.csv, market 1
df_delv1['Order_ID']=df_delv1['Order_ID'].str.replace('^YR-|0|,', '', regex=True)
df_delv1['Order_ID'].str.strip()

# %%
df_delv1.columns

# %%
# convert order id to string in orders.csv
df_ord1['Order ID']=df_ord1['Order ID'].astype('string')
# Rename column Order ID
df_ord1.rename(columns={'Order ID':'Order_ID'}, inplace=True)

# %%
# Inner Join Orders and Deliveries, Market1
df_orders_delivery1= df_ord1.merge(df_delv1, how='inner', on='Order_ID')

# %%
df_orders_delivery1.sample(10)

# %%
# Drop null rows were order id and task id is null from new merged data, market 1, orders and delivery
df_orders_delivery1.isna().sum()
# df_orders_delivery1.dropna(subset=['Task_ID', 'Order_ID'], inplace=True)

# %%
df_orders_delivery1.columns

# %% [markdown]
# Final Merge or Join of Data For Market 1

# %%
df_cust1.sample(2)

# %%
df_customers_orders_market1= df_cust1.merge(df_orders_delivery1, how='inner', on='Customer ID')

# %%
df_customers_orders_market1.sample(5)
# df_customers_orders_market1.columns

# %% [markdown]
# MARKET 2 TRANSFORMATIONS AND CLEANING

# %%


# %%
# Strip Order ID off unnecessary strings and space, market 2
df_delv2['Order_ID']=df_delv2['Order_ID'].str.replace('^YR-|0|,', '', regex=True)
df_delv2['Order_ID'].str.strip()

# %%


# %%
# strip delivery order id for market 2

df_delv2.sample(2)

# %%
# Cast Order ID to string in markrt 2 orders
df_ord2['Order ID']=df_ord2['Order ID'].astype('string')
# Inner Join Orders and Deliveries, Market2
df_orders_delivery2= df_ord2.merge(df_delv2, how='inner', left_on='Order ID', right_on='Order_ID')
#rename order ID for market 2
df_orders_delivery2.rename(columns={'Order ID': 'Order_ID_Mkt2'}, inplace = True)
df_orders_delivery2.sample(5)


# %%
# Replace non-numeric values ('-') with NaN
df_customers_orders_market1['Cost Price'].replace('-', pd.NA, inplace=True)
df_customers_orders_market1['Total Cost Price'].replace('-', pd.NA, inplace=True)

# %%
# Convert columns to float dtype, coercing errors to NaN
df_customers_orders_market1['Cost Price'] = pd.to_numeric(df_customers_orders_market1['Cost Price'], errors='coerce')
df_customers_orders_market1['Total Cost Price'] = pd.to_numeric(df_customers_orders_market1['Total Cost Price'], errors='coerce')


# %% [markdown]
# FINAL JOIN, MARKET 2, CUSTOMERS, ORDERS & DELIVERIES

# %%
df_customers_orders_market2 = df_cust2.merge(df_orders_delivery2, how='inner', on='Customer ID')

# %%
df_customers_orders_market2.sample(5)

# %%
df_customers_orders_market1.columns

# %% [markdown]
# MERGE MARKET 1 AND MARKET 2- OUTER JOIN

# %%
df = df_customers_orders_market1.merge(df_customers_orders_market2, how='outer')

# %%
df.head(10)
# df.to_csv('merged_markets.csv', index=False)

# %%
# df_cust1.dropna(subset=['',''])
df.columns

# %%
df.rename(columns={'Distance (in km)':'Distance_KM','Distance(m)':'Distance_M','Total_Time_Taken(min)':'Total_Time_Taken_min', 'Sub Total':'Sub Total_Mkt1'  }, inplace=True)

# %%
df.isna().sum()

# %%
df.drop(columns=['Is Blocked', 'Language', 'Outstanding Amount','Tax','Delivery Charge', 'Tip_x', 'Tip_y', 'Discount_x','Discount_y', 'Remaining Balance', 'Additional Charge', 'Transaction ID', 'Order Preparation Time', 'Debt Amount', 'Flat Discount', 'Checkout Template Name', 'Checkout Template Value', 'Unnamed: 30', 'Unnamed: 31'   ], inplace=True)

# %%
df.columns

# %% [markdown]
# INSTANCE OF PG DB CONNECTION

# %%
con = db_connection.pgcon
cur = db_connection.pgcur

# %%
df.columns

# %% [markdown]
# TABLE_CREATION: CREATE DATABASE TABLE BASED ON THE COLUMN NAMES OF DATAFRAME 

# %%
# map df data type to SQL table column definitions
dtype_mapping = {
    'int64': 'integer',
    'object': 'VARCHAR'
}

# Create or replace the table in PostgreSQL database
column_definitions = ', '.join([f"{re.sub('[^a-zA-Z]+', '', col.replace(' ', '_'))} {dtype_mapping.get(str(df[col].dtype), 'VARCHAR')}" for col in df.columns])

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

# %% [markdown]
# INGESTION_CODE: INSERT DATA INTO POSTGRES DB IN BATCHES OF 1000

# %%
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



