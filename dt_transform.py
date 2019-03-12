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

# Defining the immutable values
data_dir = "/home/lserra/PycharmProjects/hablaAI/data/"
data_file = "hablaAI.csv"
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


def create_dataframe(sqlc, csv, logger):
    """
    Creating a dataframe from a JSON file
    """
    logger.info("Creating a dataframe from a CSV file . . .")

    return sqlc.read.csv(csv, header="true")


def main(sqlc, logger):
    """
    Here is where everything happens
    """
    # Creating the dataframe
    df_csv = create_dataframe(sqlc, data_path, logger)

    # Show the values
    df_csv.show()
    df_csv.printSchema()

    # Saving the final result into a CSV file
    # save_to_csv(df_csv, logger)


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
