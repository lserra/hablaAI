#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
#                       D A T A       E X T R A C T I O N
# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Customer: Habla AI (Geanderson Lenz via UpWork)
# ==============================================================================
# This script extracts all data from JSON file, make a lot of stuff with all
# those data, and at the end save the results to a CSV file.
# Despite this task to be using PySpark is not necessary to be executed
# with 'spark-submit' command.
# For a better comprehension you can see in README.md.
# ==============================================================================

# Referencing all the libraries used into the code
import csv
import logging
import logging.handlers
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

# Defining the immutable values
data_dir = "/home/lserra/PycharmProjects/hablaAI/data/"
data_file = "just_ten_rows.json"
data_path = os.path.join(data_dir, data_file)

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


def create_dataframe(sqlc, json):
    """
    Creating a dataframe from a JSON file
    """
    logger.info("Creating a dataframe from a JSON file . . .")

    return sqlc.read.option("multiline", "true").json(json)


def explode_columns(df):
    """
    Exploding the columns and renaming them to a new dataframe
    """
    logger.info("Exploding the columns to a new dataframe . . .")

    df_cols_exp = df.select(
        df['_id.$binary'].alias('id_binary'),
        df['_id.$type'].alias('id_type'),
        df['CustomerId.$binary'].alias('customer_binary'),
        df['CustomerId.$type'].alias('customer_type'),
        df['UserId.$binary'].alias('user_binary'),
        df['UserId.$type'].alias('user_type'),
        df['ItemId.$binary'].alias('item_binary'),
        df['ItemId.$type'].alias('item_type'),
        df['Type.Name'].alias('type_name'),
        df['Type.Value'].alias('type_value'),
        df['Url'].alias('url'),
        df['UrlRaw'].alias('url_raw'),
        df['IsReferral'].alias('is_referral'),
        df['CreateDate.$date'].alias('create_date'),
        df['IP'].alias('ip'),
        df['IsProcessedIp'].alias('is_processed_ip'),
        df['LastClickDate.$date'].alias('last_click_date'),
        df['Parameters'].alias('parameters'),
        df['Agent.Device.Family'].alias('device_family'),
        df['Agent.Device.IsSpider'].alias('device_is_spider'),
        df['Agent.OS.Family'].alias('os_family'),
        df['Agent.Browser.Family'].alias('browser_family'),
        df['Agent.Browser.Major'].alias('browser_major'),
        df['Agent.Browser.Minor'].alias('browser_minor'),
        df['Agent.Browser.Patch'].alias('browser_patch'),
        df['IsDirect'].alias('is_direct'),
        df['Referrer'].alias('referrer')
    )

    return df_cols_exp


def split_columns(df):
    """
    Splitting columns timestamp in two columns 'date' and 'time'
    """
    logger.info("Splitting columns timestamp . . .")

    # Adding a new column: date
    df = df.withColumn('date', F.to_date(df['create_date']))

    # Adding a new column: time
    df_dt_tm = df.withColumn('hour', F.hour(df['create_date']))

    return df_dt_tm


def remove_columns(df):
    """
    Removing the columns that are not necessary
    """
    logger.info("Removing the columns not necessary . . .")
    return df.drop("parameters")


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
        'hour'
    ]


def csv_writer(row):
    """
    Writing each row from RDD into the CSV file
    """
    habla_ai = open(data_dir + 'hablaAI.csv', 'a')

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
                'hour': row[27]
            })


def save_to_csv(df):
    """
    Saving the data in a CSV file to the next steps
    """
    logger.info("Saving all data to CSV file . . .")

    habla_ai = open(data_dir + 'hablaAI.csv', 'w')

    with habla_ai:
        field_names = field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writeheader()

    for each_row in df.rdd.collect():
        csv_writer(each_row)


def main(sqlc):
    """
    Here is where everything happens
    """
    # Creating the dataframe
    df_json = create_dataframe(sqlc, data_path)

    # Creating a new dataframe with all columns exploded
    df_cols_exp = explode_columns(df_json)

    # Splitting columns timestamp in two columns 'date' and 'time'
    df_dt_tm = split_columns(df_cols_exp)

    # Removing the columns that are not necessary
    df_no_param = remove_columns(df_dt_tm)

    # Show the values
    df_no_param.show()
    df_no_param.printSchema()

    # Saving the final result into a CSV file
    save_to_csv(df_no_param)


if __name__ == '__main__':
    # Starting the logger
    logger = log_setup(app_name, log_path)
    logger.info("Starting the process . . .")

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
    main(sqlc)

    # Closing the SparkContext
    logger.info("Closing the SparkContext . . .")
    sc.stop()

    # Finishing this process
    logger.info("Process has been finished!")
    print("Process has been finished with success!")
