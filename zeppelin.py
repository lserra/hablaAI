#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
#          E T L / E L T   T A S K S   W I T H     P Y S P A R K
# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Customer: HablaAI (Geanderson Lenz via UpWork)
# ==============================================================================
# This script extracts all data from JSON file, make a lot of stuff with all
# those data, and at the end save the results to a CSV file.
# Despite this task to be using PySpark is not necessary to be executed
# with 'spark-submit' command.
# For a better comprehension you can see in README.md.
# ==============================================================================

# Referencing all the libraries used in the code
import csv
import datetime as dt
import os

from pyspark import SparkConf, SparkContext

from pyspark.sql import SQLContext
from pyspark.sql.functions import col, asc
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

from urllib.parse import urlparse

# Defining the immutable values
data_dir = "/home/lserra/PycharmProjects/hablaAI/data/"

data_file = str(dt.date.today().strftime('%Y-%m-%d')) + ".json"
data_file_input = "hablaAI.csv"
data_file_output = "hablaAIO.csv"

data_path = os.path.join(data_dir, data_file)
data_path_input = os.path.join(data_dir, data_file_input)
data_path_output = os.path.join(data_dir, data_file_output)

log_dir = "/home/lserra/PycharmProjects/hablaAI/logs/"
log_file = "ETL_hablaAI.log"
log_path = os.path.join(log_dir, log_file)

app_name = "ETL_HablaAI"

# Creating the SparkConf
conf = SparkConf().setAppName(app_name)
conf = conf.setMaster("local[*]")

# Initializing the SparkContext
sc = SparkContext(conf=conf)

# Initializing the SparkSQLContext
sqlc = SQLContext(sc)


# ==============================================================================
#                       D A T A       E X T R A C T I O N
# ==============================================================================


def extract_create_dataframe(sqlc, json):
    """
    Creating a dataframe from a JSON file
    """

    return sqlc.read.option("multiline", "true").json(json)


def extract_explode_columns(df):
    """
    Exploding the columns and renaming them to a new dataframe
    """

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


def extract_split_columns(df):
    """
    Splitting columns timestamp in two columns 'date' and 'time'
    """

    # Adding a new column: date
    df = df.withColumn('date', F.to_date(df['create_date']))

    # Adding a new column: date
    df = df.withColumn('weekday', F.dayofweek(df['create_date']))

    # Adding a new column: time
    df_dt_tm = df.withColumn('hour', F.hour(df['create_date']))

    return df_dt_tm


def extract_remove_columns(df):
    """
    Removing the columns that are not necessary
    """

    return df.drop("parameters")


def extract_field_list():
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


def extract_csv_writer(row):
    """
    Writing each row from RDD into the CSV file
    """

    habla_ai = open(data_dir + 'hablaAI.csv', 'a')

    with habla_ai:
        field_names = extract_field_list()

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


def extract_save_to_csv(df):
    """
    Saving the data in a CSV file to the next steps
    """

    habla_ai = open(data_dir + 'hablaAI.csv', 'w')

    with habla_ai:
        field_names = extract_field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writeheader()

    for each_row in df.rdd.collect():
        extract_csv_writer(each_row)


def extract_main(sqlc):
    """
    Here is where everything happens
    """

    # Creating the dataframe
    df_json = extract_create_dataframe(sqlc, data_path)

    # Creating a new dataframe with all columns exploded
    df_cols_exp = extract_explode_columns(df_json)

    # Splitting columns timestamp in two columns 'date' and 'time'
    df_dt_tm = extract_split_columns(df_cols_exp)

    # Removing the columns that are not necessary
    df_no_param = extract_remove_columns(df_dt_tm)

    # Saving the final result into a CSV file
    extract_save_to_csv(df_no_param)


def run_extract():
    """
    Running all tasks
    """
    # Calling the main function
    extract_main(sqlc)


# ==============================================================================
#                D A T A       T R A N S F O R M A T I O N
# ==============================================================================


def transform_create_dataframe(sqlc, csv):
    """
    Creating a dataframe from a JSON file
    """

    return sqlc.read.format("csv").options(header='true', delimiter=',').\
        load(csv)


def url_host(col):
    """
    TBD
    """

    obj = urlparse(col)
    result = obj.hostname

    return result


def all_transformations(df):
    """
    Based on analysis that has been done by Geanderson
    """

    transf_a = df.select(
        df['device_family'].alias('device'),
        df['os_family'].alias('os'),
        df['browser_family'].alias('browser'),
        df['referrer']
    ).fillna('0')

    udf_func = udf(url_host, StringType())

    transf_b = transf_a.withColumn(
        'referral_label',
        udf_func(transf_a.referrer)
    ).fillna('0').drop('referrer')

    columns = transf_b.columns

    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=column + "_index").fit(transf_b)
        for column in columns
    ]

    pipeline = Pipeline(stages=indexers)

    transf_c = pipeline.fit(transf_b).transform(transf_b)\
        .drop('device', 'os', 'browser', 'referral_label')

    transf_d = df.select(
        df['date'],
        df['weekday'],
        df['hour'],
        df['customer_binary'],
        df['user_binary'],
        df['url'],
        df['type_value'],
        df['referrer']
    )

    df_id1 = transf_c.withColumn("id", F.monotonically_increasing_id())
    df_id2 = transf_d.withColumn("id", F.monotonically_increasing_id())

    transf_e = df_id1.join(df_id2, "id", "inner")

    transf_f = transf_e.groupby('customer_binary', 'user_binary', 'hour') \
        .pivot('type_value') \
        .agg(F.count('url'))

    transf_g = transf_f.select(
        transf_f['customer_binary'],
        transf_f['user_binary'],
        transf_f['hour'],
        transf_f['2'].alias('product_views_hour'),
        transf_f['4'].alias('cart_views_hour'),
        transf_f['5'].alias('purchase_views_hour')
    ).fillna(0)\
        .drop('customer_binary', 'hour')

    transf_h = transf_g.join(transf_e, 'user_binary', 'inner')

    transf_i = transf_h.groupby('customer_binary', 'user_binary', 'hour') \
        .agg(F.count('url').alias("page_views_hour")) \
        .fillna(0) \
        .drop("customer_binary", 'hour')

    transf_j = transf_i.join(transf_h, 'user_binary', 'inner')

    transf_k = transf_j.groupby(
        'customer_binary', 'user_binary') \
        .agg(
        F.count("page_views_hour").alias("page_views_journey"),
        F.count("product_views_hour").alias("product_views_journey"),
        F.count("cart_views_hour").alias("cart_views_journey"),
        F.count("purchase_views_hour").alias("purchase_views_journey"),
        F.first("type_value").alias("firstviewtypeloghour")
    ) \
        .fillna(0)\
        .drop('customer_binary')

    transf_l = transf_k.join(transf_j, 'user_binary', 'inner')

    transf_m = transf_l.select(
        transf_l['customer_binary'],
        transf_l['user_binary'],
        transf_l['device_index'].alias('device'),
        transf_l['os_index'].alias('os'),
        transf_l['browser_index'].alias('browser'),
        transf_l['referral_label_index'].alias('referral_label'),
        transf_l['weekday'],
        transf_l['hour'],
        transf_l['page_views_hour'],
        transf_l['product_views_hour'],
        transf_l['cart_views_hour'],
        transf_l['purchase_views_hour'],
        transf_l['page_views_journey'],
        transf_l['product_views_journey'],
        transf_l['cart_views_journey'],
        transf_l['purchase_views_journey'],
        transf_l['firstviewtypeloghour']
    ).fillna(0)

    return transf_m


def transform_field_list():
    """
    Returning the list of fields
    """

    return [
        'customer_binary',
        'user_binary',
        'device',
        'os',
        'browser',
        'referral_label',
        'weekday',
        'hour',
        'page_views_hour',
        'product_views_hour',
        'cart_views_hour',
        'purchase_views_hour',
        'page_views_journey',
        'product_views_journey',
        'cart_views_journey',
        'purchase_views_journey',
        'firstviewtypeloghour'
    ]


def transform_csv_writer(row):
    """
    Writing each row from RDD into the CSV file
    """

    habla_ai = open(data_path_output, 'a')

    with habla_ai:
        field_names = transform_field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writerow(
            {
                'customer_binary': row[0],
                'user_binary': row[1],
                'device': row[2],
                'os': row[3],
                'browser': row[4],
                'referral_label': row[5],
                'weekday': row[6],
                'hour': row[7],
                'page_views_hour': row[8],
                'product_views_hour': row[9],
                'cart_views_hour': row[10],
                'purchase_views_hour': row[11],
                'page_views_journey': row[12],
                'product_views_journey': row[13],
                'cart_views_journey': row[14],
                'purchase_views_journey': row[15],
                'firstviewtypeloghour': row[16]
            })


def transform_save_to_csv(df):
    """
    Saving the data in a CSV file to the next steps
    """

    habla_ai = open(data_path_output, 'w')

    with habla_ai:
        field_names = transform_field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writeheader()

    for each_row in df.rdd.collect():
        transform_csv_writer(each_row)


def transform_main(sqlc):
    """
    Here is where everything happens
    """

    # Creating the dataframe
    df_csv = transform_create_dataframe(sqlc, data_path_input)

    # Making all transformation over the data
    df_final = all_transformations(df_csv)

    # Saving the final result into a CSV file
    transform_save_to_csv(df_final)


def run_transform():
    """
    Running all tasks
    """

    # Calling the main function
    transform_main(sqlc)


# ==============================================================================
#                     D A T A       L O A D I N G
# ==============================================================================


def load_field_list():
    """
    Returning the list of fields
    """
    return [
        'customer_binary',
        'device',
        'os',
        'browser',
        'referral_label',
        'weekday',
        'hour',
        'page_views_hour',
        'product_views_hour',
        'cart_views_hour',
        'purchase_views_hour',
        'page_views_journey',
        'product_views_journey',
        'cart_views_journey',
        'purchase_views_journey',
        'firstviewtypeloghour'
    ]


def load_csv_writer(row, data_path_output):
    """
    Writing each row from RDD into the CSV file
    """
    habla_ai = open(data_path_output, 'a')

    with habla_ai:
        field_names = load_field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writerow(
            {
                'customer_binary': row[0],
                'device': row[1],
                'os': row[2],
                'browser': row[3],
                'referral_label': row[4],
                'weekday': row[5],
                'hour': row[6],
                'page_views_hour': row[7],
                'product_views_hour': row[8],
                'cart_views_hour': row[9],
                'purchase_views_hour': row[10],
                'page_views_journey': row[11],
                'product_views_journey': row[12],
                'cart_views_journey': row[13],
                'purchase_views_journey': row[14],
                'firstviewtypeloghour': row[15]
            })


def load_save_to_csv(count, df_filtered):
    """
    Saving the data in a CSV file to the next steps
    """

    filename = "output/customer_"
    output_filename = data_dir + filename + str(count) + ".csv"

    habla_ai = open(output_filename, 'w')

    with habla_ai:
        field_names = load_field_list()

        writer = csv.DictWriter(habla_ai, fieldnames=field_names)
        writer.writeheader()

    for rdd in df_filtered.rdd.collect():
        load_csv_writer(rdd, output_filename)


def load_create_dataframe(sqlc, csv):
    """
    Creating a dataframe from a JSON file
    """

    return sqlc.read.format("csv").options(header='true', delimiter=',').\
        load(csv)


def load_filter_rows_by_customer(customer, df):
    """
    Filtering rows by customer then all data will be saved in a CSV file
    """

    return df.filter(
        col("customer_binary") == customer).sort(asc("weekday"), asc("hour"))


def load_main(sqlc):
    """
    Here is where everything happens
    """

    # Creating the dataframe
    df_csv = load_create_dataframe(sqlc, data_path_input)

    # Selecting distinct customers
    str_sql = """
        SELECT DISTINCT customer_binary 
        FROM customers
    """

    df_csv.registerTempTable('customers')
    df_distinct_customer = sqlc.sql(str_sql)

    # Filtering rows by customer
    count = 0
    for customer in df_distinct_customer.rdd.collect():
        count += 1
        df_filtered = load_filter_rows_by_customer(
            customer[0],
            df_csv
        )

        # Saving the final result into a CSV file
        load_save_to_csv(count, df_filtered)


def run_load():
    """
    Running all tasks
    """

    # Calling the main function
    load_main(sqlc)


# ==============================================================================
#             R U N N I N G    J O B S    A N D    T A S K S
# ==============================================================================


def execute_tasks():
    """
    Jobs/tasks to be executed
    """
    run_extract()
    run_load()
    run_transform()


if __name__ == '__main__':
    execute_tasks()
    print("Tasks has been processed!")
