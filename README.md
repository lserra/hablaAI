# hablaAI

ELT/ETL Tasks using Apache Spark and PySpark.

## The Solution

This application does the following tasks:

- **E**xtraction => extract all data from JSON file and makes a few conversion and cleanup.
- **T**ransformation => makes a lot of stuff with all those data like enrichment, aggregation, and so on.
- **L**oad => load/save all data to be storage in other format.

## How does it work?

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
@hourly /path/execute/this/script.py
```

But, if you're using the Windows you can use the _**Task Manager**_ application. 

## Executing the Tasks Manually

## Visualizing the Log

## Issues

## Next Steps

## About Me

- http://lserra.datafresh.com.br/
- http://www.datafresh.com.br/

## Support

- _suporte@datafresh.com.br_

 