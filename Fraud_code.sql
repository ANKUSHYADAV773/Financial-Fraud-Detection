create database bank;
use bank
-- 1. Detect transactions with amounts significantly higher than the average amount
SELECT 
    TransactionID, 
    AccountID, 
    Amount,
    TransactionDate,
      TransactionType,
    Location
  
FROM transactions
WHERE 
    Amount > (SELECT(AVG(Amount)* 3  FROM transactions);



-- 2. Find accounts with frequent large transactions
SELECT 
    AccountID,
    COUNT(*) AS LargeTransactionCount,
    AVG(Amount) AS AvgTransactionAmount
FROM 
    transactions
WHERE 
    Amount > 1000  -- Threshold for large transactions
GROUP BY 
    AccountID
HAVING 
    COUNT(*) > 5;  -- More than 5 large transactions

-- 3. Analyze fraudulent transactions by merchant category
SELECT 
    MerchantCategory,
    round(AVG(Amount),2)AS AvgFraudAmount,
    COUNT(*) AS FraudCount
FROM 
    transactions
WHERE 
    FraudFlag = 1
GROUP BY 
    MerchantCategory;


-- 4.Identify trends in transaction amounts over time
SELECT 
    DATE(TransactionDate) AS Date,
    round(SUM(Amount),2)AS TotalAmount,
    COUNT(*) AS TransactionCount
FROM 
    transactions
GROUP BY 
    DATE(TransactionDate)
ORDER BY 
    Date;


-- 5. Analyze fraudulent transactions by location
SELECT 
    Location,
    round(AVG(Amount),2) AS AvgFraudAmount,
    COUNT(*) AS FraudCount
FROM 
    transactions
WHERE 
    FraudFlag = 1
GROUP BY 
    Location
ORDER BY 
    FraudCount DESC;

-- 6. Compare transaction amounts before and after detected fraud
SELECT 
    t1.TransactionID AS FraudTransactionID,
    t1.AccountID,
    t1.Amount AS FraudAmount,
    t1.TransactionDate AS FraudDate,
    t2.Amount AS PreviousAmount,
    t2.TransactionDate AS PreviousDate
FROM 
    transactions t1
JOIN 
    transactions t2
ON 
    t1.AccountID = t2.AccountID
WHERE 
    t1.FraudFlag = 1
    AND t2.TransactionDate < t1.TransactionDate
    AND t2.Amount < t1.Amount
ORDER BY 
    t1.TransactionID, t2.TransactionDate DESC;


-- 7. Analyze fraud occurrences by transaction type
SELECT 
    TransactionType,
    COUNT(*) AS FraudCount,
    AVG(Amount) AS AvgFraudAmount
FROM 
    transactions
WHERE 
    FraudFlag = 1
GROUP BY 
    TransactionType;


-- 8.Identify the top 10 accounts with the highest total amount of fraudulent transactions
SELECT 
    AccountID,
    round(SUM(Amount),2) AS TotalFraudAmount,
    COUNT(*) AS FraudTransactionCount
FROM 
    transactions
WHERE 
    FraudFlag = 1
GROUP BY 
    AccountID
ORDER BY 
    TotalFraudAmount DESC
LIMIT 10;


-- 9. Retrieve recent fraudulent transactions within the last 30 days
SELECT 
    TransactionID,
    AccountID,
    Amount,
    TransactionDate,
    TransactionType,
    MerchantID
FROM 
    transactions
WHERE 
    FraudFlag = 1
    AND TransactionDate >= CURDATE() - INTERVAL 30 DAY
ORDER BY 
    TransactionDate DESC;
    
    
    -- 10. Analyze fraud occurrences by merchant ID
SELECT 
    MerchantID,
    COUNT(*) AS FraudCount,
    round(AVG(Amount),2) AS AvgFraudAmount
FROM 
    transactions
WHERE 
    FraudFlag = 1
GROUP BY 
    MerchantID
ORDER BY 
    FraudCount DESC;
    
-- 11. Using a window function to detect high-value transactions compared to a moving average:-- 
    WITH RankedTransactions AS (
    SELECT 
        TransactionID,
        AccountID,
        Amount,
        TransactionDate,
        TransactionType,
        Location,
        AVG(Amount) OVER (PARTITION BY AccountID ORDER BY TransactionDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MovingAvg
    FROM 
        transactions
)

SELECT 
    TransactionID,
    AccountID,
    Amount,
    TransactionDate,
    TransactionType,
    Location,
    MovingAvg
FROM 
    RankedTransactions
WHERE 
    Amount > MovingAvg * 2;  -- Transactions significantly higher than the moving average

-- 12.Calculate the running total of fraudulent transaction amounts using a window function:

WITH FraudulentTransactions AS (
    SELECT 
        TransactionID,
        Amount,
        TransactionDate
    FROM 
        transactions
    WHERE 
        FraudFlag = 1
)

SELECT 
    TransactionID,
    Amount,
    TransactionDate,
    SUM(Amount) OVER (ORDER BY TransactionDate) AS RunningTotal
FROM 
    FraudulentTransactions
ORDER BY 
    TransactionDate;



-- 13.Combine CTE and window functions to analyze transaction patterns over time:
WITH DailyTransactionTotals AS (
    SELECT 
        DATE(TransactionDate) AS Date,
        SUM(Amount) AS DailyTotalAmount
    FROM 
        transactions
    GROUP BY 
        DATE(TransactionDate)
)

SELECT 
    Date,
    DailyTotalAmount,
    avg(DailyTotalAmount) OVER (ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS WeeklyAvg
FROM 
    DailyTransactionTotals
ORDER BY 
    Date;

-- 14.Find the top accounts with the highest total
--  fraudulent transactions over a rolling window period:

WITH FraudulentTransactions AS (
    SELECT 
        AccountID,
        Amount,
        TransactionDate
    FROM 
        transactions
    WHERE 
        FraudFlag = 1
),

RankedFraud AS (
    SELECT 
        AccountID,
        SUM(Amount) OVER (PARTITION BY AccountID ORDER BY TransactionDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS RollingTotalFraud
    FROM 
        FraudulentTransactions
)

SELECT 
    AccountID,
    round(MAX(RollingTotalFraud),2) AS MaxRollingTotalFraud
FROM 
    RankedFraud
GROUP BY 
    AccountID
ORDER BY 
    MaxRollingTotalFraud DESC
LIMIT 10;

