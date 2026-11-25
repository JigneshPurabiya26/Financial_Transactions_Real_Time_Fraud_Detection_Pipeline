from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(
    name = 'financial_transactions_streaming.silver.customers',
    comment = 'Parsed and clean customers data'
)
@dp.expect_or_drop('valid_customer_id','customer_id IS NOT NULL')

def customers_silver() -> DataFrame:
    bronze_df = spark.readStream.table('financial_transactions_streaming.bronze.customers')

    transformed_df = (bronze_df.select(
        'customer_id',
        'first_name',
        'last_name',
        'gender',
        'age',
        'city',
        'state',
        'country',
        'annual_income',
        'customer_segment',
        to_date('account_open_date', 'yyyy-MM-dd').alias('account_open_date'),
        'risk_score',
        'preferred_spending_min',
        'preferred_spending_max',
        'preferred_city',
        'preferred_country',
        'trusted_device_id',
        'card_number',
        'card_type',
        'email',
        'transaction_limit',
        'update_timestamp',
        current_timestamp().alias('silver_ingestion_timestamp')
    ))
    
    return transformed_df