from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import happybase
from functools import reduce

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s', handlers=[
    logging.FileHandler("app.log"),
    logging.StreamHandler()
])

def create_spark_connection():
    spark = None
    try:
        spark = SparkSession.builder \
                .appName('SparkDataETL') \
                .config("spark.jars.packages", "org.apache.hbase:hbase-client:2.4.9,org.apache.hadoop:hadoop-common:3.3.1") \
                .getOrCreate()
        logging.info('Spark session created succesfully')
    except Exception as e:
        logging.error(f'Could not create connection dur to {e}')
    return spark

def read_table(path, spark):
    try:
        df = spark.read.option("header", "true") \
                  .csv(path)
        return df
    except Exception as e:
        logging.error(f'Could not read data due to {e}')

def drop_missing_values(df: DataFrame, name: str):
    total_rows = df.count()
    missing_rows = df.filter(reduce(lambda a, b: a | b, [col(c).isNull() for c in df.columns])).count()
    percentage = (missing_rows/total_rows)*100
    df = df.dropna()
    
    if percentage >5:
        logging.error(f'{name} contains more than {percentage}% of missing values. Please you need to handle them!!')
    else:
        logging.info(f'{name} contains less than 5% of missing values, we just dropped them.')
    
    return df

def date_conversion(df: DataFrame, col):
    try:
        df = df.withColumn(col, to_timestamp(col))
        logging.info(f'{col} has been converted successfully.')
    except Exception as e:
        logging.error(f'Could not convert the column into date due to {e}')
    return df

def interaction_duration(df: DataFrame):
    df_duration = df.groupBy("client_id") \
                    .agg(sum("duration_seconds")) \
                    .alias('total_interaction_duration')
    return df_duration

def write_to_hbase(df: DataFrame, table_name: str):
    try:
        connection = happybase.Connection('localhost', port=32770)
        # connection.open()

        if table_name.encode() not in connection.tables():
            connection.create_table(table_name, {'cf': dict()})

        table = connection.table(table_name)
        for row in df.collect():
            row_dict = row.asDict()
            row_key = str(row_dict.pop('client_id'))  
            table.put(row_key, {f'cf:{k}': str(v) for k, v in row_dict.items()})
        
        connection.close()
        logging.info(f'Data written to HBase table {table_name} successfully')
    except Exception as e:
        logging.error(f'Could not write to HBase due to {e}')

def close_session(spark):
    spark.stop()

        
if __name__ == '__main__':
    spark = create_spark_connection()
    if spark is not None:
        df_interactions_clients = read_table('data_source/interactions_clients.csv', spark)
        df_navigation_web = read_table('data_source/navigation_web.csv', spark)
        df_produits_souscrits = read_table('data_source/produits_souscrits.csv', spark)
        
        list_df = [df_interactions_clients, df_navigation_web, df_produits_souscrits]
        list_name = ['df_interactions_clients', 'df_navigation_web', 'df_produits_souscrits']
        
        for i, df in enumerate(list_df):
            df = drop_missing_values(df, list_name[i])
            if list_name[i] == 'df_produits_souscrits':
                df = date_conversion(df, 'subscription_date')
            else:
                df = date_conversion(df, 'timestamp')
        df_duration_by_client = interaction_duration(df_interactions_clients)
        df_navigation_web_by_client = interaction_duration(df_navigation_web)
        
        write_to_hbase(df_interactions_clients, 'interactions_clients')
        write_to_hbase(df_navigation_web, 'navigation_web')
        write_to_hbase(df_produits_souscrits, 'produits_souscrits')
        write_to_hbase(df_duration_by_client, 'duration_by_client')
        write_to_hbase(df_navigation_web_by_client, 'duration_navigation_by_client')
        
        close_session(spark)
        
        
    
    