#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# ETL Spark Application with PySpark -> to be execute with spark-submit
# Customer: Habla AI (Geanderson Lenz via UpWork)
# This script load all data from CSV file to Hadoop Distributed File System
# ==============================================================================

# Referencing all the libraries used into the code
import csv
import logging
import logging.handlers
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, asc

# Defining the immutable values
data_dir = "/home/lserra/PycharmProjects/hablaAI/data/"
data_file_input = "hablaAIO.csv"
data_path_input = os.path.join(data_dir, data_file_input)

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


def field_list():
    """
    Returning the list of fields
    """
    return [
        'customer_binary',
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


def csv_writer(row, data_path_output):
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
    logger.info("Saving all data by customer to CSV file . . .")

    count = 1
    for each_row in df.rdd.collect():
        filename = "customer_" + str(count) + ".csv"

        data_path_output = os.path.join(
            data_dir,
            "output",
            filename
        )

        habla_ai = open(data_path_output, 'w')

        # with habla_ai:
        field_names = field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writeheader()

        habla_ai.close()

        csv_writer(each_row, data_path_output)

        count += 1


def create_dataframe(sqlc, csv, logger):
    """
    Creating a dataframe from a JSON file
    """
    logger.info("Creating a dataframe from a CSV file . . .")

    return sqlc.read.format("csv").options(header='true', delimiter=',').\
        load(csv)


def filter_rows_by_customer(logger, customer, df):
    """
    Filtering rows by customer then all data will be saved in a CSV file
    """
    logger.info("Filtering rows by customer . . .")

    return df.filter(
        col("customer_binary") == customer).sort(asc("weekday"), asc("hour"))


def main(sqlc, logger):
    """
    Here is where everything happens
    """
    # Creating the dataframe
    df_csv = create_dataframe(sqlc, data_path_input, logger)

    # Selecting distinct customers
    str_sql = """
        SELECT DISTINCT customer_binary 
        FROM customers
    """

    df_csv.registerTempTable('customers')
    df_distinct_customer = sqlc.sql(str_sql)

    # Filtering rows by customer
    for customer in df_distinct_customer.rdd.collect():
        print(customer[0])
        df_filtered = filter_rows_by_customer(
            logger,
            customer[0],
            df_csv
        )

        # Saving the final result into a CSV file
        save_to_csv(df_filtered, logger)


def run():
    """
    Running all tasks
    """
    # Starting the logger
    logger = log_setup(app_name, log_path)
    logger.info("Starting the load's process [T] . . .")

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
