import logging
import random
import time
from typing import List

from airflow.decorators import task
from airflow.models import Variable


TASK_COUNT = 5


@task
def determine_seed(dag_run=None) -> int:
    conf = dag_run.conf or {}
    # First get from Variables
    seed = Variable.get("seed", deserialize_json=True, default_var=0)
    # Override with context if provided, defaulting to current time if not
    seed = conf.get("seed") or seed or int(time.time())
    return seed


@task.branch
def should_continue(seed: int) -> str:
    if seed % 2 == 0:
        return "do_not_continue"
    return "generate_numbers"


@task
def generate_numbers(seed: int) -> List[int]:
    random.seed(seed)
    numbers = [random.randrange(100) for _ in range(TASK_COUNT)]
    return numbers


@task(retries=0)
def process_number(number: int) -> int:
    if number % 3 == 0:
        raise ValueError(f"Cannot process value: {number}")
    return number**2


@task
def report_numbers(numbers: List[int]) -> None:
    total = 0
    for number in numbers:
        print(f"{number=}")
        total += number
    print(f"Total value computed: {total}")
