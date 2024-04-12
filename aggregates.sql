-- Total Transaction per user
SELECT 
    customerid, 
    SUM(CAST(TRIM(ordertotal, '"') AS NUMERIC))::INTEGER AS total_transaction_amount
FROM 
    public.customersaledata
GROUP BY 
    customerid;

-- Top 50 clients by total amount
SELECT 
    customerid, 
    SUM(CAST(TRIM(ordertotal, '"') AS NUMERIC))::INTEGER AS total_transaction_amount
FROM 
    public.customersaledata
GROUP BY 
    customerid
ORDER BY 
    total_transaction_amount DESC
LIMIT 
    50;

