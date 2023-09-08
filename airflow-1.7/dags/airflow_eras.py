from __future__ import print_function

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
    "catchup": False,
}

dag = DAG("airflow_eras", default_args=default_args, schedule_interval=None)

# t1, t2 and t3 are examples of tasks created by instantiating operators
# t1 = BashOperator(
#     task_id='print_date',
#     bash_command='date',
#     dag=dag)

# Variable (then conf) as seed
# Branch based on odd or even?
# Generate 5 random numbers using seed
# Each task does some "process" on them (fail if % 3?)
# Gather all the numbers using DummyOperator and flow control
# Report available numbers

get_seed = PythonOperator(
    task_id="get_seed",
    python_callable=funcs.determine_seed,
    provide_context=True,
    dag=dag,
)

determine_continue = BranchPythonOperator(
    task_id="should_continue",
    python_callable=funcs.should_continue,
    provide_context=True,
    dag=dag,
)

workflow_stopped = PythonOperator(
    task_id="do_not_continue",
    python_callable=print,
    op_args=["Workflow has been skipped!"],
    dag=dag,
)

generate = PythonOperator(
    task_id="generate_numbers",
    python_callable=funcs.generate_numbers,
    provide_context=True,
    dag=dag,
)

gather = DummyOperator(
    task_id="gather_numbers",
    trigger_rule="all_done",
    dag=dag,
)

for index in range(funcs.TASK_COUNT):
    process = PythonOperator(
        task_id="process_number_{}".format(index),
        python_callable=funcs.process_number,
        provide_context=True,
        op_kwargs={"index": index},
        dag=dag,
        retries=0,
    )
    generate.set_downstream(process)
    process.set_downstream(gather)

report = PythonOperator(
    task_id="report_numbers",
    python_callable=funcs.report_numbers,
    provide_context=True,
    dag=dag,
)


get_seed.set_downstream(determine_continue)
determine_continue.set_downstream(workflow_stopped)
determine_continue.set_downstream(generate)
gather.set_downstream(report)
