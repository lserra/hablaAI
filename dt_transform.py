#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
#                  D A T A       T R A N S F O R M A T I O N
# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Customer: Habla AI (Geanderson Lenz via UpWork)
# ==============================================================================
# This script extracts all data from JSON file, make a lot of stuff with all
# those data, and at the end save the results to a CSV file.
# ==============================================================================

# Referencing all the libraries used into the code
import csv
import logging
import logging.handlers
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
# from pyspark.sql.functions import col
from datetime import datetime, timedelta

# Defining the immutable values
data_dir = "/home/lserra/PycharmProjects/hablaAI/data/"
data_file_input = "hablaAI.csv"
data_file_output = "hablaAIO.csv"
data_path_input = os.path.join(data_dir, data_file_input)
data_path_output = os.path.join(data_dir, data_file_output)

log_dir = "/home/lserra/PycharmProjects/hablaAI/logs/"
log_file = "ETL_hablaAI.log"
log_path = os.path.join(log_dir, log_file)

app_name = "ETL_HablaAI"


def log_setup(app_name, log_path):
    """
    Logging all the steps executed by the ETL
    """
    # Logger initialisation
    logger = logging.getLogger(app_name)

    # Create console handler and set level to debug
    file_handler = logging.FileHandler("{}".format(log_path))
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(pathname)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Setting the Logger Level (INFO)
    logger.setLevel(logging.INFO)

    return logger


def create_dataframe(sqlc, csv, logger):
    """
    Creating a dataframe from a JSON file
    """
    logger.info("Creating a dataframe from a CSV file . . .")

    return sqlc.read.format("csv").options(header='true', delimiter=',').\
        load(csv)


def register_temp_table(logger, df):
    """
    Registering the schema as a temp table
    """
    logger.info("Registering the schema as a temp table . . .")

    df.registerTempTable("interactions")


def execute_sql(sqlc, sql):
    """
    TBD
    """
    return sqlc.sql(sql)


def all_transformations(logger, df):
    """
    TBD
    """
    logger.info("Making a few transformations over data . . .")

    df1 = df.select(
        df['device_family'].alias('device'),
        df['os_family'].alias('os'),
        df['browser_family'].alias('browser')
    )

    columns = ["device", "os", "browser"]
    indexers = [
        StringIndexer(inputCol=column, outputCol=column + "_index").fit(df1)
        for column in columns
    ]

    pipeline = Pipeline(stages=indexers)

    df_r = pipeline.fit(df1).transform(df1)
    df_r.drop('device', 'os', 'browser')

    df_type_id = df1.select(df['type_value'])

    df_id1 = df_r.withColumn("id", F.monotonically_increasing_id())
    df_id2 = df_type_id.withColumn("id", F.monotonically_increasing_id())

    df3 = df_id1.join(df_id2, "id", "inner")
    df3.drop('device', 'os', 'browser')

    result_2 = df1.withColumn("id", F.monotonically_increasing_id())

    result_3 = result_2.join(df3, 'id', 'inner')
    # result_3.drop('Type', 'Agent')

    result_4 = result_3.select(result_3['date'])

    result_5 = result_4.select(
        result_4['date'],
        result_4['weekday'],
        result_4['hour']
    )

    result_6 = result_5.withColumn("id", F.monotonically_increasing_id())

    result_7 = result_3.join(result_6, 'id', 'inner')

    result_8 = result_7.groupby('customer_binary', 'user_binary', 'hour')\
        .pivot('type_value')\
        .agg(F.count('url'))

    result_9 = result_8.toDF(
        'customer_binary',
        'user_binary',
        'hour',
        'product_views_hour',
        'cart_views_hour',
        'purchase_views_hour').fillna(0).drop('customer_binary', 'hour')

    result_10 = result_9.join(result_7, 'user_binary', 'inner')
    # result_10.drop('type_value')

    result_11 = result_10.groupby('customer_binary', 'user_binary', 'hour')\
        .agg(F.count('url').alias("page_views_hour"))\
        .fillna(0)\
        .drop("customer_binary", 'hour')

    result_12 = result_10.join(result_11, 'user_binary', 'inner').drop('url')

    last_week = (datetime.today() - timedelta(days=7)).strftime(fmt='%Y-%m-%d')

    journey = result_12.where(result_12.date <= last_week)

    result_13 = journey.groupby('customer_binary', 'user_binary')\
        .agg(
        F.count("page_views_hour").alias("page_views_journey"),
        F.count("product_views_hour").alias("product_views_journey"),
        F.count("cart_views_hour").alias("cart_views_journey"),
        F.count("purchase_views_hour").alias("purchase_views_journey")
    )\
        .fillna(0)\
        .drop('customer_binary')

    result_14 = result_12.join(result_13, 'user_binary', 'inner')
    result_15 = result_14.select(
        result_14['device_index'].alias('device'),
        result_14['os_index'].alias('os'),
        result_14['browser_index'].alias('browser'),
        result_14['weekday'],
        result_14['hour'],
        result_14['product_views_hour'],
        result_14['cart_views_hour'],
        result_14['purchase_views_hour'],
        result_14['page_views_hour'],
        result_14['product_views_journey'],
        result_14['cart_views_journey'],
        result_14['purchase_views_journey'],
        result_14['page_views_journey']
    )

    return result_15


def field_list():
    """
    Returning the list of fields
    """
    return [
        'id_binary',
        'id_type',
        'customer_binary',
        'customer_type',
        'user_binary',
        'user_type',
        'item_binary',
        'item_type',
        'type_name',
        'type_value',
        'url',
        'url_raw',
        'is_referral',
        'create_date',
        'ip',
        'is_processed_ip',
        'last_click_date',
        'device_family',
        'device_is_spider',
        'os_family',
        'browser_family',
        'browser_major',
        'browser_minor',
        'browser_patch',
        'is_direct',
        'referrer',
        'date',
        'weekday',
        'hour'
    ]


def csv_writer(row):
    """
    Writing each row from RDD into the CSV file
    """
    habla_ai = open(data_path_output, 'a')

    with habla_ai:
        field_names = field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writerow(
            {
                'id_binary': row[0],
                'id_type': row[1],
                'customer_binary': row[2],
                'customer_type': row[3],
                'user_binary': row[4],
                'user_type': row[5],
                'item_binary': row[6],
                'item_type': row[7],
                'type_name': row[8],
                'type_value': row[9],
                'url': row[10],
                'url_raw': row[11],
                'is_referral': row[12],
                'create_date': row[13],
                'ip': row[14],
                'is_processed_ip': row[15],
                'last_click_date': row[16],
                'device_family': row[17],
                'device_is_spider': row[18],
                'os_family': row[19],
                'browser_family': row[20],
                'browser_major': row[21],
                'browser_minor': row[22],
                'browser_patch': row[23],
                'is_direct': row[24],
                'referrer': row[25],
                'date': row[26],
                'weekday': row[27],
                'hour': row[28]
            })


def save_to_csv(df, logger):
    """
    Saving the data in a CSV file to the next steps
    """
    logger.info("Saving all data to CSV file . . .")

    habla_ai = open(data_path_output, 'w')

    with habla_ai:
        field_names = field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writeheader()

    for each_row in df.rdd.collect():
        csv_writer(each_row)


def main(sqlc, logger):
    """
    Here is where everything happens
    """
    # Creating the dataframe
    df_csv = create_dataframe(sqlc, data_path_input, logger)

    # Registering the schema as a temp table
    # register_temp_table(logger, df_csv)

    # TBD
    # sql_query = """
    #         SELECT type_name, type_value
    #         FROM interactions
    #         WHERE customer_type = '03'
    #     """
    #
    # df_tmp_tbl = execute_sql(sqlc, sql_query)

    df_final = all_transformations(logger, df_csv)

    # Show the values
    df_final.show()
    df_final.printSchema()

    # Saving the final result into a CSV file
    # save_to_csv(df_final, logger)


def run():
    """
    Running all tasks
    """
    # Starting the logger
    logger = log_setup(app_name, log_path)
    logger.info("Starting the transformation's process [T] . . .")

    # Creating the SparkConf
    logger.info("Creating the SparkConf . . .")
    conf = SparkConf().setAppName(app_name)
    conf = conf.setMaster("local[*]")

    # Initializing the SparkContext
    logger.info("Initializing the SparkContext . . .")
    sc = SparkContext(conf=conf)

    # Initializing the SparkSQLContext
    logger.info("Initializing the SparkSQLContext . . .")
    sqlc = SQLContext(sc)

    # Calling the main function
    main(sqlc, logger)

    # Closing the SparkContext
    logger.info("Closing the SparkContext . . .")
    sc.stop()

    # Finishing this process
    logger.info("Process has been finished!")


if __name__ == '__main__':
    run()
