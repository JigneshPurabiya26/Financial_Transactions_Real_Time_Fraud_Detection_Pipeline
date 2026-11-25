from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
import json

@dp.table(
    name = 'financial_transactions_streaming.bronze.transactions',
    comment = 'Transactions raw stream data ingested from kafka'
)
def transactions_bronze() -> DataFrame:

    kafka_connection_json = dbutils.secrets.get(scope = 'financial-Transactions-scope-3',key = 'kafka_connection_details')
    kafka_config = json.loads(kafka_connection_json)
    bootstrap_server = kafka_config['bootstrap_server']
    api_key = kafka_config['api_key']
    api_secret = kafka_config['api_secret']
    topic = kafka_config['topic']
    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'


    streaming_df= (spark.readStream.format('kafka')
                .option("kafka.bootstrap.servers",bootstrap_server)
                .option("subscribe",topic)
                .option("kafka.security.protocol", "SASL_SSL")  #this and the below three lines are for authentication
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas_config)
                .option("startingOffsets","earliest") #this is to read the data that is available from the very beginning
                .load()
                )
    
    parsed_stream = (streaming_df.select(
                                col("key").cast("string"),
                                col("value").cast("string"),
                                col("topic"),
                                col("partition"),
                                col("offset"),
                                col("timestamp"),
                                col("timestampType"))
                                .withColumn('ingestion_timestamp',current_timestamp())
                                )
    
    return parsed_stream