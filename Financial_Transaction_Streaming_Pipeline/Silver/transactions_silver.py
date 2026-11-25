from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.table(
    name = 'financial_transactions_streaming.silver.transactions',
    comment = 'Parsed and clean transaction data'
)
@dp.expect_or_drop('valid_transactions','transaction_id IS NOT NULL')
@dp.expect_or_drop('valid_customer_id','customer_id IS NOT NULL')
@dp.expect_or_drop('valid_card_number','card_number IS NOT NULL')
@dp.expect_or_drop('valid_merchant_id','merchant_id IS NOT NULL')
@dp.expect('valid_amount','amount > 0')
def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table('financial_transactions_streaming.bronze.transactions')

    schema = StructType([
                        StructField("transaction_id", StringType(), True),
                        StructField("customer_id", StringType(), True),
                        StructField("card_number", StringType(), True),
                        StructField("merchant_id", StringType(), True),
                        StructField("merchant_name", StringType(), True),
                        StructField("merchant_category", StringType(), True),
                        StructField("amount", DoubleType(), True),
                        StructField("currency", StringType(), True),
                        StructField("transaction_type", StringType(), True),
                        StructField("payment_channel", StringType(), True),
                        StructField("device_id", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("transaction_timestamp", TimestampType(), True),
                        StructField("is_international", BooleanType(), True),
                        StructField("status", StringType(), True)
                        ])

    transformed_df = (bronze_df.select(
        from_json(col('value'),schema).alias('data'),
        col("topic").alias('Kafka_topic'),
        col("partition").alias('Kafka_partition'),
        col("offset").alias('Kafka_offset'),
        col("timestamp").alias('Kafka_timestamp'),
        col("ingestion_timestamp").alias('bronze_ingestion_timestamp')
    ).select(
        col('data.*'),
        col('Kafka_topic'),
        col('Kafka_partition'),
        col('Kafka_offset'),
        col('Kafka_timestamp'),
        col('bronze_ingestion_timestamp'))
    .withColumn('silver_ingestion_timestamp',current_timestamp())
    )
    
    return transformed_df