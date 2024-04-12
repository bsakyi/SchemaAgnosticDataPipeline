# Data Pipeline Engineering 

## Task Overview -

You are tasked with designing an advanced data pipeline for a large e-commerce platform. The provided data simulates real-world scenarios with complexities such as data schema evolution, incremental updates, and handling various data formats. The goal is to build a scalable, fault-tolerant data pipeline that efficiently processes the provided data for analytics purposes.  Please clone this repo to your machine, work on your solution and then upload the full repo to your own profile on Github and share it with us. You can see more detailed instructions in the submission section below. **Do not open a PR to this repo!!!** 

## Dataset Description -

The provided dataset consists of three types of data from two distinct markets:

### Customer Data - Market 1 & 2 (`customers.json`)

Contains information about user transactions, including Customer ID, Last Used Platform, Is Blocked, Created At, Language, Outstanding Amount, Loyalty Points, Number of employees

### Orders Data - Market 1 & 2 (`order.csv`)

Contains orders data, including Order ID, Order Status, Category Name, SKU, Customization Group, Customization Option, Quantity, Unit Price, Cost Price, Total Cost Price, Total Price, Order Total, Sub Total, Tax, Delivery, Charge, Tip, Discount, Remaining Balance, Payment Method, Additional Charge, Taxable Amount, Transaction ID, Currency Symbol, Transaction Status, Promo Code, Customer ID, Merchant ID, Description, Distance (in km), Order Time, Pickup Time, Delivery Time, Ratings, Reviews, Merchant Earning, Commission Amount, Commission Payout Status, Order Preparation Time, Debt Amount, Redeemed Loyalty Points, Consumed Loyalty Points, Cancellation Reason, Flat Discount, Checkout Template Name, Checkout Template Value

### Deliveries Data - Market 1 & 2 (`deliveries.csv`)

Contains deliveries data, including Task_ID, Order_ID, Relationship, Team_Name, Task_Type, Notes, Agent_ID, Agent_Name, Distance(m), Total_Time_Taken(min), Task_Status, Ref_Images, Rating, Review, Latitude, Longitude, Tags, Promo_Applied, Custom_Template_ID, Task_Details_QTY, Task_Details_AMOUNT, Special_Instructions, Tip, Delivery_Charges, Discount, Subtotal, Payment_Type, Task_Category, Earning, Pricing

## Requirements -

### Data Ingestion and Schema Evolution:

- Implement a data ingestion mechanism to load data from the CSV and JSON files
- Handle schema evolution scenarios, where the data schema may change over time.

### Incremental Updates:

- Design the data pipeline to support incremental updates, allowing it to process only new or modified records since the last execution.
- Consider scenarios where the data is continuously updated.

### Data Transformation:

- Implement data transformations to enrich the orders and delivery data with user information.
- Calculate aggregate metrics, such as total transaction amount per user. Bonus points for including as many aggregations as possible suitable for an eCommerce platform.

### Scalability and Parallel Processing:

- Design the pipeline to be scalable, supporting parallel processing for improved performance.
- Utilize any relevant technology or framework (e.g., Apache Spark) to achieve scalability.

### Data Storage:

- Choose a suitable database or storage solution to store the processed data. Explain your choice based on factors like scalability, data structure, and query requirements.
- Implement the storage mechanism, ensuring it supports efficient retrieval and querying.

### Fault Tolerance:

- Implement fault-tolerant mechanisms to handle failures during data processing.
- Ensure that the pipeline can recover gracefully from errors without losing processed data.

### Documentation:

- Provide clear and comprehensive documentation explaining your data pipeline's architecture, data flow, and any significant design decisions.
- Include instructions on how to run and maintain the pipeline.

## Submission Guidelines -
#### Please clone this repo to your machine, work on your solution and then upload the full repo to your own profile on Github. Please, don't open a PR to this repo!
1. Upload to a private repo under your personal Github profile.
2. Open a PR against it with your solution.
3. Transfer the ownership of the repo to the account of the person you were in contact with (ask us in case you are unsure about this - should be provided in the email you received).
#### Details:
- We appreciate if you could git commit each relevant step of your development.
- Share your source code, scripts, and any configuration files.
- Include a `README_YOUR_NAME.md` file with instructions on how to set up and run your data pipeline.
- If using any specific technologies or libraries, specify the versions used.


## Evaluation Criteria -

- **Functionality:** Does the data pipeline process and store data accurately?
- **Scalability:** Is the solution scalable to handle larger datasets and increased processing loads?
- **Fault Tolerance:** How well does the pipeline handle errors and failures during processing?
- **Documentation:** Is the `README_YOUR_NAME.md` comprehensive and easy to follow?
- **Code Quality:** Is the code well-organized, readable, and maintainable?
