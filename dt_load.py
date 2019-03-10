#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# ETL Spark Application with PySpark -> execute with spark-submit
# Customer: Habla AI (Geanderson Lenz via UpWork)
# This script load all data from CSV file to Hadoop Distributed File System
# ==============================================================================
import sys, os
import shutil
import zipfile
import subprocess

import time
import logging
import logging.handlers

APPNAME = "Wishhack.Staging"
LOGDIR = "/mnt/hadoop/disk01/custom_logs/staging/"
LOGFILE = "file_ingestion.log"
LOGPATH = os.path.join(LOGDIR, LOGFILE)
INBOUND = "/mnt/hadoop/disk03/data/inbound/"
PROCESSING = "/mnt/hadoop/disk03/data/processing"
PROCESSED = "/mnt/hadoop/disk03/data/processed"
HDFS_STG = "/data/staging/<file_type>"


def log_setup(appname, logpath):
    log_handler = logging\
        .handlers\
        .TimedRotatingFileHandler(
        logpath, when='D',
        interval=1,
        backupCount=7)

    formatter = logging\
        .Formatter(
        '%(asctime)s - %(name)s - %(pathname)s - %(funcName)s - %(levelname)s - %(message)s',
        '%Y-%m-%d %H:%M:%S')

    log_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    return logger


logger = log_setup(APPNAME, LOGPATH)


def runCommand(cmd):
    logger.info("Running " + cmd)
    rt = 0
    rt = subprocess.call(cmd, shell=True)
    if rt <> 0:
        logger.error("Command Failed:" + cmd)
        exit(rt)
    return rt


def hive_drop_partition_cmd(stg_table, partition_id):
    hivecmd = "alter table <stg_table>  drop if exists partition (pt='<partition_id>');".replace(
        "<partition_id>", partition_id).replace("<stg_table>", stg_table)
    shellcmd = 'hive -e "' + hivecmd + '"'
    return shellcmd


def hive_add_partition_cmd(stg_table, partition_id, partition_location):
    hivecmd = "alter table <stg_table> ADD PARTITION(pt='<partition_id>') location '<partition_location>';".replace(
        "<partition_location>", partition_location).replace("<partition_id>",
                                                            partition_id).replace(
        "<stg_table>", stg_table)
    shellcmd = 'hive -e "' + hivecmd + '"'
    return shellcmd


def getHDFSLocation(file_type, filebase=None):
    if not filebase:
        return HDFS_STG.replace("<file_type>", file_type)
    else:
        return HDFS_STG.replace("<file_type>", file_type) + "/" + filebase


def hdfs_upload_cmd(file_type, localDir):
    HDFS_STG_FULL = getHDFSLocation(file_type)
    return "hdfs dfs -copyFromLocal -f <localDir> <HDFS_STG_FULL>".replace(
        "<HDFS_STG_FULL>", HDFS_STG_FULL).replace("<localDir>", localDir)


def unzip_single_file(path_to_zip_file, directory_to_extract_to):
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(directory_to_extract_to)
    zip_ref.close()


def getFilePath(file_type, file_name, location=INBOUND):
    return os.path.join(location, file_type, file_name)


def printer(msg, val):
    print
    msg + " : " + val


def prep_for_hdfs_single_file(file_type, file_name):
    inbound_zip_file = getFilePath(file_type, file_name, INBOUND)
    processing_zip_file = getFilePath(file_type, file_name, PROCESSING)
    processed_zip_file = getFilePath(file_type, file_name, PROCESSED)

    shutil.copyfile(inbound_zip_file, processing_zip_file)
    filebase = file_name.split(".")[0]
    unzip_dir = os.path.join(PROCESSING, file_type, filebase)
    print
    unzip_dir
    unzip_single_file(processing_zip_file, unzip_dir)
    return (filebase, unzip_dir, inbound_zip_file, processing_zip_file,
            processed_zip_file)


def cleanup(unzip_dir, inbound_zip_file, processing_zip_file,
            processed_zip_file):
    try:
        shutil.move(inbound_zip_file, processed_zip_file)
    except:
        logger.error(
            "Could not move " + inbound_zip_file + " to " + processed_zip_file)
        raise

    try:
        os.remove(processing_zip_file)
        if unzip_dir.startswith(PROCESSING):
            shutil.rmtree(unzip_dir)

    except:
        logger.error("Could not cleanup Processing Folder")
        raise


def main():
    file_type = sys.argv[1]  # "wishlist"
    stg_table = sys.argv[2]  # "wishhack.wishlist_stg"
    logger.info("Initiating Staging load for %s, %s" % (file_type, stg_table))
    inbound_folder = os.path.join(INBOUND, file_type)
    arr = [x for x in os.listdir(inbound_folder)]
    sarr = sorted(arr, key=lambda x: os.path.getmtime(
        os.path.join(inbound_folder, x)))
    print
    sarr

    for f in sarr:
        print
        f
        logger.info("Processing: " + f)
        logger.info("Unzipping: ")
        filebase, unzip_dir, inbound_zip_file, processing_zip_file, processed_zip_file = prep_for_hdfs_single_file(
            file_type, f)
        upload_cmd = hdfs_upload_cmd(file_type, unzip_dir)
        logger.info("***** Uploading to HDFS *****")
        runCommand(upload_cmd)
        # hdc = hive_drop_partition_cmd(stg_table=stg_table,partition_id=filebase)
        # print "***** Dropping Partition *****"
        # print runCommand(hdc)

        hac = hive_add_partition_cmd(stg_table=stg_table, partition_id=filebase,
                                     partition_location=getHDFSLocation(
                                         file_type, filebase))
        logger.info("Hive Command:" + hac)
        logger.info("***** Adding Partition *****")
        runCommand(hac)
        logger.info("***** Cleaning up *****")
        cleanup(unzip_dir, inbound_zip_file, processing_zip_file,
                processed_zip_file)
        logger.info("*********** Completed processing for " + f + " ********")


if __name__ == "__main__":
    main()
