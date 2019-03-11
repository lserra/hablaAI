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
import logging
import logging.handlers
import os

# from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

# Defining the immutable values
data_dir = "/home/lserra/PycharmProjects/hablaAI/"
data_file = "data/just_ten_rows.json"
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


def creating_df(sqlc, json):
    """
    Creating a dataframe from a JSON file
    """
    logger.info("Creating a dataframe from a JSON file . . .")

    return sqlc.read.option("multiline", "true").json(json)


def exploding_cols(df):
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


def split_date_time(df):
    """
    Splitting the column 'create_date' in two columns 'date' and 'time'
    """
    logger.info("Splitting the column 'create_date' . . .")

    # Adding a new column: date
    df = df.withColumn('date', F.to_date(df['create_date']))

    # Adding a new column: time
    df_dt_tm = df.withColumn('hour', F.hour(df['create_date']))

    return df_dt_tm


def remove_parameters(df):
    """
    Removing the column 'parameters'
    """
    return df.drop("parameters")


def main(sqlc):
    """
    Here is where everything happens
    """
    # Creating the dataframe
    df_json = creating_df(sqlc, data_path)

    # Creating a new dataframe with all columns exploded
    df_cols_exp = exploding_cols(df_json)

    # Splitting the column 'create_date' in two columns 'date' and 'time'
    df_dt_tm = split_date_time(df_cols_exp)

    # Removing the column 'parameters'
    df_no_param = remove_parameters(df_dt_tm)

    # Show the values
    df_no_param.show()
    df_no_param.printSchema()

    # Save the results in CSV file
    # logger.info("Saving all data to CSV file . . .")
    # df_dt_tm.write.csv(
    #     os.path.join(data_path, "hablaAI.csv"),
    #     header='true'
    # )


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
