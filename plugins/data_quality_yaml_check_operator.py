import logging
from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook

import yaml

class DataQualityYAMLCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 yaml_path,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.yaml_path = Path(yaml_path)
        with open(self.yaml_path) as configs:
            self.configs = yaml.full_load(configs)
        self.conn_type = self.configs.get("fields").get("conn_type")
        self.conn_id = self.configs.get("fields").get("conn_id")
        self.sql = self.configs.get("fields").get("sql")
        self.check_description = self.configs.get("check_description")
        self.eval_threshold = self.configs.get("threshold").get("eval_threshold", False)
        self.min_threshold = self.configs.get("threshold").get("min_threshold")
        self.max_threshold = self.configs.get("threshold").get("max_threshold")
        self.threshold_conn_type = self.configs.get("threshold").get("threshold_conn_type")
        self.threshold_conn_id = self.configs.get("threshold").get("threshold_conn_id")
        self.push_conn_type = self.configs.get("fields").get("push_conn_type")
        self.push_conn_id = self.configs.get("fields").get("push_conn_id")

    def check_conn(self, conn):
        if conn not in {"postgres", "mysql", "hive"}:
            raise ValueError(f"""Connection type {conn} not currently supported""")

    def check_sql(self, sql):
        for keyword in {"INSERT INTO", "UPDATE", "DROP", "ALTER"}:
            if keyword in sql.upper():
                raise Exception(f"""Cannot use the keyword {keyword} in a sql query:\n{sql}""")
    @property
    def conn_type(self):
        return self._conn_type

    @property
    def threshold_conn_type(self):
        return self._threshold_conn_type

    @property
    def sql(self):
        return self._sql

    @property
    def min_threshold(self):
        return self._min_threshold

    @property
    def max_threshold(self):
        return self._max_threshold

    @conn_type.setter
    def conn_type(self, conn):
        self.check_conn(conn)
        self._conn_type = conn

    @threshold_conn_type.setter
    def threshold_conn_type(self, conn):
        self.check_conn(conn)
        self._threshold_conn_type = conn

    @sql.setter
    def sql(self, sql):
        self.check_sql(sql)
        self._sql = sql

    @min_threshold.setter
    def min_threshold(self, sql):
        if self.eval_threshold:
            self.check_sql(sql)
        self._min_threshold = sql

    @max_threshold.setter
    def max_threshold(self, sql):
        if self.eval_threshold:
            self.check_sql
        self._max_threshold = sql

    def _get_hook(self, conn_type, conn_id):
        if conn_type == "postgres":
            return PostgresHook(postgres_conn_id=conn_id)
        if conn_type == "mysql":
            return MySqlHook(mysql_conn_id=conn_id)
        if conn_type == "hive":
            return HiveServer2Hook(hiveserver2_conn_id=conn_id)

    def get_result(self, conn_type, conn_id, sql):
        hook = self._get_hook(conn_type, conn_id)
        result = hook.get_records(sql)
        if len(result) > 1:
            logging.info("Result: %s contains more than 1 entry", str(result))
            raise ValueError("Result from sql query contains more than 1 entry")
        if len(result) < 1:
            raise ValueError("No result returned from sql query")
        if len(result[0]) != 1:
            logging.info("Result: %s does not contain exactly 1 column", str(result[0]))
            raise ValueError("Result from sql query does not contain exactly 1 column")
        return result[0][0]

    def execute(self, context):
        result = self.get_result(self.conn_type, self.conn_id, self.sql)
        info_dict = {
            "result" : result,
            "description" : self.check_description,
            "task_id" : self.task_id,
            "execution_date" : context['execution_date']
        }

        if self.eval_threshold:
            upper_threshold = self.get_result(self.threshold_conn_type, self.threshold_conn_id, self.max_threshold)
            lower_threshold = self.get_result(self.threshold_conn_type, self.threshold_conn_id, self.min_threshold)
        else:
            upper_threshold = self.max_threshold
            lower_threshold = self.min_threshold

        if lower_threshold < result < upper_threshold:
            info_dict["within_threshold"] = True
            return info_dict
        info_dict["within_threshold"] = False
        return info_dict

class DataQualityYAMLCheckPlugin(AirflowPlugin):
    name = "data_quality_yaml_check_operator"
    operators = [DataQualityYAMLCheckOperator]
