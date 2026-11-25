from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


@dp.table(
    name = 'financial_transactions_streaming.gold.high_value_transactions_alert',
    comment = 'Alert details where the transaction has been performed higher than the value set by the customer'
)
def high_value_transactions_alert() -> DataFrame:

    transactions = spark.readStream.table('financial_transactions_streaming.silver.transactions') 
    customers = spark.read.table('financial_transactions_streaming.silver.customers')

    joined_df = (transactions.join(customers,transactions.customer_id == customers.customer_id,"left")
                .filter(col('amount')>col('transaction_limit'))
                .select(
                    concat_ws('_',lit('Alert'),col('transaction_id')).alias('alert_id'),
                    lit('High Value Transaction').alias('alert_type'),
                    current_timestamp().alias('alert_timestamp'),
                    transactions.transaction_id,
                    transactions.customer_id,
                    col('customers.email').alias('customers_email'),
                    concat_ws(" ",col('first_name'),col('last_name')).alias('customer_name'),
                    col('transactions.amount').alias('transaction_amount'),
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.transaction_type,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status
                ))
    
    return joined_df