import logging
import random
import time

from airflow.models import Variable


TASK_COUNT = 5

log = logging.getLogger(__name__)


def determine_seed(**context):
    # First get from Variables
    seed = Variable.get("seed", deserialize_json=True, default_var=0)
    # Override with context if provided, defaulting to current time if not
    conf = context["dag_run"].conf or {}
    seed = conf.get("seed", seed or int(time.time()))
    return seed


def should_continue(**context):
    seed = context["ti"].xcom_pull("get_seed")
    if seed % 2 == 0:
        return "do_not_continue"
    return "generate_numbers"


def generate_numbers(**context):
    seed = context["ti"].xcom_pull("get_seed")
    random.seed(seed)
    numbers = [random.randrange(100) for _ in range(TASK_COUNT)]
    return numbers


def process_number(index, **context):
    number = context["ti"].xcom_pull("generate_numbers")[index]
    if number % 3 == 0:
        raise ValueError("Cannot process value: {}".format(number))
    return number**2


def report_numbers(**context):
    total = 0
    for index in range(TASK_COUNT):
        number = context["ti"].xcom_pull("process_number_{}".format(index)) or 0
        total += number
    log.info("Total value computed: {}".format(total))
