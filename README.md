# csv-parser
a CSV parser that reads,parse and pushes it to a SQL database
It uses polars (an alternative to pandas but written in rust) to read and parse csv files, keeps what has been written successfuly or not into a json file called status.json.

## status file
the file is a json file that keeps track of what has been read,parsed and written to the database. because it runs on a cronjob, i needed a mechanism to check if the CSV file has been read or not so if the script ran once again, the already parsed csv wont get parsed twice or more and also for debugging porpuses.
the status file schema looks like this:
``` { <filename>: true|false, } ```.
the true or false indicates wether the file has been successfully read and written to database or not
