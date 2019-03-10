#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# ETL Spark Application with PySpark -> execute with spark-submit
# Customer: Habla AI (Geanderson Lenz via UpWork)
# ==============================================================================
import dt_extract as from_json
import dt_load as to_hdfs


def run_tasks():
    """
    List of tasks/jobs to be executed by Spark through the command spark-submit
    """
    from_json.main()
    to_hdfs


if __name__ == '__main__':
