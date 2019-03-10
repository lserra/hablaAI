#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Task: ETL using PySpark -> execute with spark-submit
# Customer: Habla AI (Geanderson Lenz via UpWork)
# This script extracts all data from JSON file, make a lot of stuff with all
# those data, and at the end save the results to a CSV file.
# ==============================================================================
import csv
import io
import logging
import logging.handlers

# import matplotlib.pyplot as plt

from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
from pyspark import SparkConf, SparkContext

# Defining the immutable values
PATH = "/home/lserra/PycharmProjects/hablaAI/data/just_ten_rows.json"
APP_NAME = "HablaAI ETL PySpark Submit"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

fields = ('date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
          'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance')

Flight = namedtuple('Flight', fields)


def parse(row):
    """
    Parse a row and returns a named tuple
    """
    row[0] = datetime.strptime(row[0], DATE_FMT).date()
    row[5] = datetime.strptime(row[5], TIME_FMT).time()
    row[6] = float(row[6])
    row[7] = datetime.strptime(row[7], TIME_FMT).time()
    row[8] = float(row[8])
    row[9] = float(row[9])
    row[10] = float(row[10])

    return Flight(*row[:11])


def split(line):
    """
    Operator function for splitting a line with CSV module
    """
    reader = csv.reader(io.StringIO(line))
    # return reader.next() -> deprecated
    return next(reader)


# def plot(delays):
#     """
#     Show a bar chart of the total delay per airline
#     """
#     airlines = [d[0] for d in delays]
#     minutes = [d[1] for d in delays]
#     index = list(range(len(airlines)))
#
#     fig, axe = plt.subplots()
#     bars = axe.barh(index, minutes)
#
#     # Add the total minutes to the right
#     for idx, air, min in zip(index, airlines, minutes):
#         if min > 0:
#             bars[idx].set_color('#d9230f')
#             axe.annotate(" %0.0f min" % min, xy=(min + 1, idx + 0.5),
#                          va='center')
#         else:
#             bars[idx].set_color('#469408')
#             axe.annotate(" %0.0f min" % min, xy=(10, idx + 0.5), va='center')
#
#     # Set the ticks
#     ticks = plt.yticks([idx + 0.5 for idx in index], airlines)
#     xt = plt.xticks()[0]
#     plt.xticks(xt, [' '] * len(xt))
#
#     # Minimize chart junk
#     plt.grid(axis='x', color='white', linestyle='-')
#
#     plt.title("Total Minutes delayed per Airline")
#     plt.show()


def main(sc):
    """
    Main functionality
    """
    # Load the airlines lookup dictionary
    airlines = dict(sc.textFile("/home/df/Temp/ontime/airlines.csv")
                    .map(split).collect())

    # Broadcast the lookup dictionary to the cluster
    airline_lookup = sc.broadcast(airlines)

    # Read the CSV data into a RDD
    flights = sc.textFile(PATH).map(split).map(
        parse)

    # Map the total delay to the airline
    # (joined using the broadcast value)
    delays = flights.map(lambda f: (
        airline_lookup.value[f.airline],
        add(f.dep_delay, f.arv_delay)
    ))

    # Reduce the total delay for the month to the airline
    delays = delays.reduceByKey(add).collect()
    delays = sorted(delays, key=itemgetter(1))

    # Provide output from the driver
    for d in delays:
        print("{} minutes delayed\t{}".format(d[1], d[0]))

    # Show a bar chart of the delays
    # plot(delays)


if __name__ == '__main__':
    # Setting SparkConf
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    # Initializing a Context
    sc = SparkContext(conf=conf)

    # Call Main functionality
    main(sc)

    sc.stop()
