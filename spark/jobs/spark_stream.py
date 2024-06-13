import logging
import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_cassandra_keyspace(cassandra_session, keyspace_name: str):
    cassandra_session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS "{keyspace_name}"
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
    """)


def create_cassandra_table(cassandra_session):
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT
        )
    """)


def create_spark_connection():
    spark_con = None
    try:
        spark_con = SparkSession.builder.appName('SparkDataStreaming') \
            .config('spark.jars.packages',
                    'com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,'
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,'
                    'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0') \
            .config('spark.jars.repositories', 'https://repo1.maven.org/maven2/') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        spark_con.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection successfully created')
    except Exception as e:
        logging.error(f'Couldn\'t create spark connection due to exception: {e}')

    return spark_con


def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f'Could not create Cassandra connection due to the error: {e}')
        return None


def connect_to_kafka(spark_connection_):
    try:
        df_ = spark_connection_.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        return df_
    except Exception as e:
        logging.warning(f'Couldn\'t create Spark dataframe due to the error: {e}')


def transform_for_cassandra(df_):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('postcode', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
    ])

    return df_.selectExpr('CAST(value AS STRING)') \
        .select(from_json(col('value'), schema).alias('data')).select('data.*')


if __name__ == '__main__':
    spark_connection = create_spark_connection()
    if spark_connection is None:
        exit(1)

    raw_df = connect_to_kafka(spark_connection)
    transformed_df = transform_for_cassandra(raw_df)

    cassandra_session = create_cassandra_connection()
    if cassandra_session is None:
        exit(1)
    create_cassandra_keyspace(cassandra_session, 'spark_streams')
    create_cassandra_table(cassandra_session)

    streaming_query = transformed_df.writeStream.format('org.apache.spark.sql.cassandra') \
                                    .option('checkpointLocation', '/tmp/checkpoint') \
                                    .option('keyspace', 'spark_streams') \
                                    .option('table', 'users') \
                                    .start()

    streaming_query.awaitTermination()
