# hablaAI

ELT/ETL jobs and tasks using Apache Spark and PySpark.

## The Solution

This application does the following tasks:

- **E**_xtraction_ => extract all data from JSON file and makes a few conversion and cleanup.
- **T**_ransformation_ => makes a lot of stuff with all those data like enrichment, aggregation, and so on.
- **L**_oad_ => load/save all data to be storage in other format.

## How it works

![HablaAI Dataflow](/home/lserra/PycharmProjects/hablaAI/image/hablaAI.png "HablaAI Dataflow")

## Scheduling the Task

It's very easy to schedule a task in Linux.

Linux provides a scheduling utility called _**crontab**_. This utility allows that scripts can be run periodically. 
You can run any command, executable or script file at any time and day you require, and as many times as you like. 

For example, you can use the _**crontab**_ to schedule a task using this command:

```shell
$ crontab -e
```

This will open the default editor to let us manipulate the _**crontab**_. 
If you save and exit the editor, all your _**cronjobs**_ are saved into _**crontab**_. 

_**Cronjobs**_ are written in the following format:

```shell
* * * * * /path/execute/this/script.py
```

_What are those asterisks?_ 
So this means, execute my script every minute, of every hour, of every day of the month, of every month, and every day in the week.
This script will be executed every minute, without exception.

To see what _**cronjob**_ are currently running on your system, you can open a terminal and run this command:

```shell
$ crontab -l
```

Also, you can use any of the following for configuring how often to run a command:

```
@hourly    Once an hour at the beginning of the hour

@daily     Once a day at midnight

@weekly    Once a week at midnight on Sunday morning

@monthly   Once a month at midnight on the morning of the first day of the month

@yearly    Once a year at midnight on the morning of January 1

@reboot    At startup
```

Instead of to use asterisks you could use:

```shell
@hourly ../path/application/hablaAI/run_jobs_spark.py
```

But, if you're using the Windows you can use the _**Task Manager**_ application. 

## Executing the Task Manually
This application is compound for 3 modules. Those modules can be executed individually. For example: something went wrong with the extraction's process.
You can run only this script again. Each module have a script associated with it.

- extraction : you can run the script "_**dt_extract.py**_"
- transformation : you can run the script "_**dt_transform.py**_"
- loading : you can run the script "_**dt_load.py**_"

To run anyone, you can use the following command in the Linux terminal:

```shell
$ python ../path/application/hablaAI/dt_extract.py
```

## Visualizing the Log
When the application is running is generated a log of all activities.
And this log is stored in a file "_**ETL_hablaAI.log**_" that can be found in the following path:

```shell
$ cd ../path/application/hablaAI/logs/
```

To visualize the 10 first rows from this file, you can use the following command in the Linux terminal:

```shell
hablaAI/logs$ head ETL_hablaAI.log
```

To visualize the 10 last rows from this file, you can use the following command in the Linux terminal:

```shell
hablaAI/logs$ tail ETL_hablaAI.log
```

```csv
2019-03-14 22:47:39,969 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Filtering rows by customer . . .
2019-03-14 22:47:40,033 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Saving all data by customer to CSV file . . .
2019-03-14 22:47:40,107 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Filtering rows by customer . . .
2019-03-14 22:47:40,119 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Saving all data by customer to CSV file . . .
2019-03-14 22:47:40,226 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Filtering rows by customer . . .
2019-03-14 22:47:40,303 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Saving all data by customer to CSV file . . .
2019-03-14 22:47:40,390 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Filtering rows by customer . . .
2019-03-14 22:47:40,424 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Saving all data by customer to CSV file . . .
2019-03-14 22:47:40,524 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Closing the SparkContext . . .
2019-03-14 22:47:40,838 - ETL_HablaAI - /home/lserra/PycharmProjects/hablaAI/dt_load.py - INFO - Process has been finished!

```

Also, you can open this file using the following command in the Linux terminal:

```shell
hablaAI/logs$ nano ETL_hablaAI.log 
```

## Requirements

There are a few python libraries that must be installed before to run this application.
You can find out all the libraries specified into the file "_**requirements.txt**_".
To install those libraries you can run the following command in the Linux terminal:

```shell
$ pip install -r requirements.txt 
```  

## Issues
No issues until the moment.
But if you find out any issues, please contact us.

## Next Steps
Refactoring some pieces of the code to improve better performance, and makes easier 
the maintenance using OO programming. 

## About Me

- http://lserra.datafresh.com.br/
- http://www.datafresh.com.br/

## Support

- [suporte@datafresh.com.br](suporte@datafresh.com.br)

 
