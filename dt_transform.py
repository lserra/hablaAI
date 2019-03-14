#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
#                D A T A       T R A N S F O R M A T I O N S
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


def all_transformations(logger, df):
    """
    Based on analysis that has been done by Geanderson
    """
    logger.info("Making transformations over all data . . .")

    last_week = (datetime.today() - timedelta(days=7))\
        .strftime(format='%Y-%m-%d')

    transf_a = df.select(
        df['device_family'].alias('device'),
        df['os_family'].alias('os'),
        df['browser_family'].alias('browser')
    )

    columns = transf_a.columns
    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=column + "_index").fit(transf_a)
        for column in columns
    ]

    pipeline = Pipeline(stages=indexers)

    transf_b = pipeline.fit(transf_a).transform(transf_a)\
        .drop('device', 'os', 'browser')

    transf_c = df.select(
        df['date'],
        df['weekday'],
        df['hour'],
        df['customer_binary'],
        df['user_binary'],
        df['url'],
        df['type_value']
    )

    df_id1 = transf_b.withColumn("id", F.monotonically_increasing_id())
    df_id2 = transf_c.withColumn("id", F.monotonically_increasing_id())

    transf_d = df_id1.join(df_id2, "id", "inner")

    transf_e = transf_d.groupby('customer_binary', 'user_binary', 'hour') \
        .pivot('type_value') \
        .agg(F.count('url'))

    # result_9 = transf_e.toDF(
    transf_f = transf_e.select(
        transf_e['customer_binary'],
        transf_e['user_binary'],
        transf_e['hour'],
        transf_e['2'].alias('product_views_hour'),
        transf_e['4'].alias('cart_views_hour'),
        transf_e['5'].alias('purchase_views_hour')
    ).fillna(0)\
        .drop('customer_binary', 'hour')

    transf_g = transf_f.join(transf_d, 'user_binary', 'inner')\
        .drop('type_value')

    transf_h = transf_g.groupby('customer_binary', 'user_binary', 'hour') \
        .agg(F.count('url').alias("page_views_hour")) \
        .fillna(0) \
        .drop("customer_binary", 'hour')

    transf_i = transf_g.join(transf_h, 'user_binary', 'inner').drop('url')

    df_journey = transf_i.where(transf_i.date <= last_week)

    transf_j = df_journey.groupby(
        'customer_binary', 'user_binary') \
        .agg(
        F.count("page_views_hour").alias("page_views_journey"),
        F.count("product_views_hour").alias("product_views_journey"),
        F.count("cart_views_hour").alias("cart_views_journey"),
        F.count("purchase_views_hour").alias("purchase_views_journey")
    ) \
        .fillna(0)\
        .drop('customer_binary')

    transf_j = transf_i.join(transf_j, 'user_binary', 'inner')

    transf_k = transf_j.select(
        transf_j['customer_binary'],
        transf_j['user_binary'],
        transf_j['device_index'].alias('device'),
        transf_j['os_index'].alias('os'),
        transf_j['browser_index'].alias('browser'),
        transf_j['weekday'],
        transf_j['hour'],
        transf_j['product_views_hour'],
        transf_j['cart_views_hour'],
        transf_j['purchase_views_hour'],
        transf_j['page_views_hour'],
        transf_j['product_views_journey'],
        transf_j['cart_views_journey'],
        transf_j['purchase_views_journey'],
        transf_j['page_views_journey']
    ).fillna(0)

    return transf_k


def field_list():
    """
    Returning the list of fields
    """
    return [
        'customer_binary',
        'user_binary',
        'device',
        'os',
        'browser',
        'weekday',
        'hour',
        'product_views_hour',
        'cart_views_hour',
        'purchase_views_hour',
        'page_views_hour',
        'product_views_journey',
        'cart_views_journey',
        'purchase_views_journey',
        'page_views_journey'
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
                'customer_binary': row[0],
                'user_binary': row[1],
                'device': row[2],
                'os': row[3],
                'browser': row[4],
                'weekday': row[5],
                'hour': row[6],
                'product_views_hour': row[7],
                'cart_views_hour': row[8],
                'purchase_views_hour': row[9],
                'page_views_hour': row[10],
                'product_views_journey': row[11],
                'cart_views_journey': row[12],
                'purchase_views_journey': row[13],
                'page_views_journey': row[14]
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

    df_final = all_transformations(logger, df_csv)

    # Show the values
    df_final.show()
    df_final.printSchema()

    # Saving the final result into a CSV file
    save_to_csv(df_final, logger)


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
