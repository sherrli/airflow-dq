from airflow.plugins_manager import AirflowPlugin
from airflow.model import BaseOperator
from airflow.utils.decorators import apply_defaults
import yaml

class DataQualityYAMLCheckOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 yaml_path
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





class DataQualityYAMLCheckPlugin(AirflowPlugin):
    name = "data_quality_yaml_check_operator"
    operator = [DataQualityYAMLCheckOperator]