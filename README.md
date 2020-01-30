# airflow-dq

## Introduction
An Airflow module that includes operators to perform data quality checks. This module includes an Airflow operator that periodically check the results of a query and validates it against a specified threshold.  

Optionally: 

 - If it exceeds the specified threshold level and an Airflow operator that sends an alert through email about given threshold breach
 - Results can be pushed to an external database to store the execution results history

## Features
This Airflow module contains the following operators:
- `BaseDataQualityOperator`
- `DataQualityThresholdCheckOperator`
- `DataQualityThresholdCheckSQLOperator`

### BaseDataQualityOperator
`BaseDataQualityOperator` found in [`base_data_quality_operator.py`](plugins/base_data_quality_operator.py) is derived from `BaseOperator` and is used as an inherited class in the other operators. This operator shares common attributes with the operators. These functions include:
- `get_sql_value()` - Given database connection requirements, method will evaluate a result of a sql query and return if and only if is a single column, single row result.
- `send_failure_notification()` - If emails are provided for the task, method will send an email with specifications of the data quality test run when it fails.
- `push()` - **(Optional)** A user-defined method that allows ability to export metadata from a data quality check to an external database for logging.

### DataQualityThresholdCheckOperator
`DataQualityThresholdCheckOperator` found in [`data_quality_threshold_check_operator.py`](plugins/data_quality_threshold_check_operator.py) inherits from `BaseDataQualityOperator`. It is used to perform the data quality check against a threshold range. 

In this operator, the data quality check executes using `get_sql_value()`. Thresholds are given as numeric values and the test will determine if the data quality result is within that range. If configured, `push()` will then be called to send metadata and test results to an external datastore. If the result is within the threshold range, the task passes and the metadata is returned. Otherwise if the result is outside the thresholds, `send_failure_notification()` is called to log the failed test and email users if necessary.

#### Usage
An example of this operator looks like this:
```python
task = DataQualityThresholdCheckOperator(
    task_id="task_check_average_value",
    sql="SELECT AVG(value) FROM Price;",
    conn_id="postgres_connection",
    min_threshold=20,
    max_threshold=50,
    push_conn_id="push_conn",
    check_description="test to determine whether the average of the Price table is between 20 and 50",
    dag=dag
)
```
The parameters used are:
- `sql` - data quality check sql statement
- `conn_id` - connection id of the location where the sql statement should execute
- `min_threshold` and `max_threshold` - the range in which the data quality result should lie
- `push_conn_id` - (optional) connection id of external table that logs data quality results
- `check_description` - (optional) description text of the test being run

### DataQualityThresholdCheckSQLOperator
`DataQualityThresholdCheckSQLOperator` found in [`data_quality_threshold_sql_check_operator.py`](plugins/data_quality_threshold_sql_check_operator.py) inherits from `BaseDataQualityOperator`. It is almost identical to `DataQualityThresholdCheckOperator`, however the only difference is that instead of passing threshold values into the operator, the operator will take in sql statements for the threshold.

The operator will collect the sql statement for the min and max threshold and before performing the data quality check, it will evaluate these sql statements using `get_sql_value()`. After collecting these threshold values, the operator will evaluate the data quality test and check against the thresholds and determine if the result lies outside or inside the threshold. If the result is within the threshold range, the task passes and the metadata is returned. Otherwise if the result is outside the thresholds, `send_failure_notification()` is called to log the failed test and email users if necessary.

#### Usage
An example of this operator looks like this:
```python
task = DataQualityThresholdCheckOperator(
    task_id="task_check_average_value",
    sql="SELECT AVG(value) FROM Price;",
    conn_id="postgres_connection",
    threshold_conn_id="threshold_postgres_id",
    min_threshold_sql="SELECT MIN(price) FROM Sales WHERE date>=(NOW() - interval '1 month');",
    max_threshold_sql="Select MAX(price) FROM Sales WHERE date>=(NOW() - interval '1 month');",
    push_conn_id="push_conn",
    check_description="test to of whether the average of Price table is between low and high of Sales table from the last month",
    dag=dag
)
```
The parameters used are:
- `sql` - data quality check sql statement
- `conn_id` - connection id of the location where the sql statement should execute
- `threshold_conn_id` - connection id of the location where the threshold sql statements should execute
- `min_threshold_sql` and `max_threshold_sql` - sql statements to define the range in which the data quality result should lie
- `push_conn_id` - (optional) connection id of external table that logs data quality results
- `check_description` - (optional) description text of the test being run


### YAML Usage and other examples
Example DAG usages are also provided in this package located in the [`example_dags/`](example_dags/) directory. This directory includes usages of both types of Threshold Check Operators. There will also be a DAG-level implementation of how YAML files could be used as test configurations for each operator.

## Tests
Tests can be found [here](tests/). Test directory gives an outline of each test file and the purpose of each. Additionally, it contains test configurations such as a sql script that creates test tables and configuration YAML files.

## Flowchart Diagrams
Diagrams below visualize flow of execution when `DataQualityThresholdCheckOperator` and `DataQualityThresholdSQLCheckOperator` are signaled to execute.

### DataQualityThresholdCheckOperator Flowchart
![data_quality_threshold_check_operator diagram](operator_diagrams/data_quality_threshold_check_operator_flowchart.png)

### DataQualityThresholdSQLCheckOperator Flowchart
![data_quality_threshold_sql_check_operator diagram](operator_diagrams/data_quality_threshold_sql_check_operator_flowchart.png)

