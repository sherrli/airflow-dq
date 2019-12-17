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
        with open(self.yaml_path) as config:
            self.config = yaml.full_load(config)
        self.conn_type = self.config.get("fields").get("conn_type")
        self.conn_id = self.config.get("fields").get("conn_id")
        self.sql = self.config.get("fields").get("sql")
        self.check_description = self.config.get("check_description")
        self.eval_threshold = self.config.get("threshold").get("eval_threshold", False)
        self.min_threshold = self.config.get("threshold").get("min_threshold")
        self.max_threshold = self.config.get("threshold").get("max_threshold")
        self.threshold_conn_type = self.config.get("threshold").get("threshold_conn_type")
        self.threshold_conn_id = self.config.get("threshold").get("threshold_conn_id")





class DataQualityYAMLCheckPlugin(AirflowPlugin):
    name = "data_quality_yaml_check_operator"
    operator = [DataQualityYAMLCheckOperator]