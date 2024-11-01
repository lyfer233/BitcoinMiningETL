import json
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.smtp.notifications.smtp import send_smtp_notification


@dag(
    dag_id="bitcoin_mining",
    schedule_interval=timedelta(seconds=1),
    start_date=pendulum.datetime(2024, 11, 1, tz="CST"),
    catchup=False,
    tags=["mining"],
    on_failure_callback=[
        send_smtp_notification(
            from_email="deepwind32@163.com",
            to="2734829381@qq.com",
            subject="[Error] The dag {{ dag.dag_id }} failed",
            html_content="debug logs",
        )
    ],
    default_args={
        "last_load_time": 0
    }
)
def tutorial_taskflow_api():
    """
    """

    @task()
    def extract():
        """
        #### Extract task

        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task

        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        #### Load task

        """
        # Do nothing when first run.
        if Variable.get('last_load_time') - time.time() > 5 * 60:
            print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


tutorial_taskflow_api()
