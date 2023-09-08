from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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


XCOM_PULL = "{{ ti.xcom_pull('%s') }}"


with DAG(
    "airflow_eras",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
) as dag:
    get_seed = PythonOperator(
        task_id="get_seed",
        python_callable=funcs.determine_seed,
        op_kwargs={
            "conf": "{{ dag_run.conf or {} }}",
        },
    )

    determine_continue = BranchPythonOperator(
        task_id="should_continue",
        python_callable=funcs.should_continue,
        op_kwargs={
            "seed": XCOM_PULL % get_seed.task_id,
        },
    )

    workflow_stopped = PythonOperator(
        task_id="do_not_continue",
        python_callable=lambda x: print(x),
        op_args=["Workflow has been skipped!"],
    )

    generate = PythonOperator(
        task_id="generate_numbers",
        python_callable=funcs.generate_numbers,
        op_kwargs={
            "seed": XCOM_PULL % get_seed.task_id,
        },
    )

    gather = DummyOperator(
        task_id="gather_numbers",
        trigger_rule=TriggerRule.NONE_SKIPPED,
    )

    for index in range(funcs.TASK_COUNT):
        process = PythonOperator(
            task_id=f"process_number_{index}",
            python_callable=funcs.process_number,
            op_kwargs={
                "index": index,
                "numbers": XCOM_PULL % generate.task_id,
            },
            retries=0,
        )
        generate >> process >> gather

    report = PythonOperator(
        task_id="report_numbers",
        python_callable=funcs.report_numbers,
    )

    get_seed >> determine_continue >> [workflow_stopped, generate]
    gather >> report
