def process_sum_transactions(spark):
    sum_transactions = spark.sql("""
    WITH 
    iofs AS (
    SELECT 
        'income' AS type,
        id AS sales_id,
        branch_id,
        CAST(NULL AS INT) AS employee_id,
        CONCAT('Penjualan ', product_name, ' sejumlah ', quantity) AS description,
        order_date AS date,
        amount
    FROM fact_sales
    WHERE is_online_transaction = false
    ), 
    ions AS (
    SELECT 
        'income' AS type,
        id AS sales_id,
        branch_id,
        CAST(NULL AS INT) AS employee_id,
        CONCAT('Penjualan ', product_name, ' sejumlah ', quantity) AS description,
        order_date AS date,
        price * quantity AS amount
    FROM fact_sales
    WHERE is_online_transaction = true
    ),
    oos AS (
    SELECT 
        'outcome' AS type,
        id AS sales_id,
        branch_id,
        CAST(NULL AS INT) AS employee_id,
        'Pengeluaran untuk biaya ongkir' AS description,
        order_date AS date,
        delivery_fee AS amount
    FROM fact_sales
    WHERE is_free_delivery_fee = 'true'
    ),
    ods AS (
    SELECT 
        'outcome' AS type,
        id AS sales_id,
        branch_id,
        CAST(NULL AS INT) AS employee_id,
        CONCAT('Pengeluaran diskon ', disc_name) AS description,
        order_date AS date,
        price * quantity * disc / 100 AS amount
    FROM fact_sales
    WHERE disc IS NOT NULL
    )
    SELECT * FROM iofs
    UNION ALL
    SELECT * FROM ions
    UNION ALL
    SELECT * FROM oos
    UNION ALL
    SELECT * FROM ods
    """)

    return sum_transactions
