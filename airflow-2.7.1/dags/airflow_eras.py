from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

import funcs


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule=None,
    params={"seed": Param(None)},
    render_template_as_native_obj=True,
)
def airflow_eras():
    seed = funcs.determine_seed()

    determine_continue = funcs.should_continue(seed)

    @task
    def do_not_continue():
        print("Workflow has been skipped!")

    generate = funcs.generate_numbers(seed)

    determine_continue >> [do_not_continue(), generate]

    processers = funcs.process_number.expand(number=generate)

    gather = EmptyOperator(
        task_id="gather_numbers",
        trigger_rule=TriggerRule.NONE_SKIPPED,
    )

    report = funcs.report_numbers.override(trigger_rule=TriggerRule.NONE_SKIPPED)(
        processers
    )

    processers >> gather >> report


airflow_eras()
