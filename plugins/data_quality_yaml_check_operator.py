from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import yaml
from pathlib import Path

class DataQualityYAMLCheckOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 yaml_path,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.yaml_path = Path(yaml_path)
        try:
            with open(self.yaml_path) as configs:
                self.configs = yaml.full_load(configs)
        except FileNotFoundError:
            raise
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
    
        print(self.configs)

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
    
    def execute(self, context):
        pass




class DataQualityYAMLCheckPlugin(AirflowPlugin):
    name = "data_quality_yaml_check_operator"
    operators = [DataQualityYAMLCheckOperator]