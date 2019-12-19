from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from data_quality_threshold_check_operator import DataQualityThresholdCheckOperator

import yaml

class DataQualityYAMLCheckOperator(DataQualityThresholdCheckOperator):
    '''
    DataQualityYAMLCheckOperator runs loads configuration parameters from a yaml
    file and runs a data quality check based off the specifications in the file

    :param yaml_path: path to yaml configuration file with specifications for the test
    :type yaml_path: str
    '''

    @apply_defaults
    def __init__(self,
                 yaml_path,
                 *args,
                 **kwargs):
        # super().__init__(*args, **kwargs)
        self.yaml_path = Path(yaml_path)
        with open(self.yaml_path) as configs:
            conf = yaml.full_load(configs)
        super().__init__(conn_type=conf.get("fields").get("conn_type"),
                         conn_id=conf.get("fields").get("conn_id"),
                         sql=conf.get("fields").get("sql"),
                         check_description=conf.get("check_description"),
                         eval_threshold=conf.get("threshold").get("eval_threshold"),
                         min_threshold=conf.get("threshold").get("min_threshold"),
                         max_threshold=conf.get("threshold").get("max_threshold"),
                         threshold_conn_type=conf.get("threshold").get("threshold_conn_type"),
                         threshold_conn_id=conf.get("threshold").get("threshold_conn_id"),
                         push_conn_type=conf.get("fields").get("push_conn_type"),
                         push_conn_id=conf.get("fields").get("push_conn_id"),
                         *args,
                         **kwargs)

    def execute(self, context):
        return super().execute(context=context)

class DataQualityYAMLCheckPlugin(AirflowPlugin):
    name = "data_quality_yaml_check_operator"
    operators = [DataQualityYAMLCheckOperator]
