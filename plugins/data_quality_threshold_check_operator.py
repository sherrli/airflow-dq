from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from base_data_quality_operator import BaseDataQualityOperator, get_sql_value

class DataQualityThresholdCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdCheckOperator builds off BaseOperator and
    executes a data quality check against high & low threshold values.

    :param min_threshold: lower-bound value
    :type min_threshold: numeric
    :param max_threshold: upper-bound value
    :type max_threshold: numeric
    """

    @apply_defaults
    def __init__(self,
                 min_threshold,
                 max_threshold,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    def execute(self, context):
        result = get_sql_value(self.conn_id, self.sql)
        info_dict = {
            "result" : result,
            "description" : self.check_description,
            "task_id" : self.task_id,
            "execution_date" : context.get("execution_date"),
            "min_threshold" : self.min_threshold,
            "max_threshold" : self.max_threshold,
            "within_threshold" : self.min_threshold <= result <= self.max_threshold
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key=f"""result data from task {self.task_id}""", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict

class DataQualityThresholdCheckPlugin(AirflowPlugin):
    name = "data_quality_threshold_check_operator"
    operators = [DataQualityThresholdCheckOperator]
