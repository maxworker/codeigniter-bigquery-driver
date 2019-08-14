# Codeigniter Google BigQuery database driver

Allows using standard Codeigniter database features to access Google BigQuery. Only read-only access supported ('select' based expressions).
This library uses Standard SQL, check - [Enabling Standard SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql)
Additionally, "insert_id" function redefined to get a job id.

## Dependencies
* php cloud-bigquery library
* php php-sql-parser library
* active Google BigQuery dataset

## Installation
Install dependencies to 'application/third_party':
```
composer require google/cloud-bigquery
composer require greenlion/php-sql-parser
```
Copy the 'BigQuery.php' library file in 'application/libraries' and copy drivers to 'system/database/drivers/bigquery' (or use Codeigniter Dynamic Database Connection helper and do not modify the 'system' folder).

You must to set database connection params: define 'dbdriver' as 'bigquery', define 'hostname' as name of engine, 'username' as key file and 'database' as dataset name (create a database configuration group in the 'database.php' config file and use it as the third parameter when loading a model).

## Sample
See the 'Test_bq_model' sample, it uses [Codeigniter Dynamic Database Connection Helper](https://github.com/maxworker/codeigniter-dynamicdb). 

## License
Copyright (c) 2016 Max Butenko
Codeigniter-bigquery-driver is licensed under MIT license.
