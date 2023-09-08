import logging
import random
import time
from typing import List

from airflow.models import Variable


TASK_COUNT = 5


def determine_seed(conf: dict) -> int:
    # First get from Variables
    seed = Variable.get("seed", deserialize_json=True, default_var=0)
    # Override with context if provided, defaulting to current time if not
    seed = conf.get("seed", seed or int(time.time()))
    return seed


def should_continue(seed: int) -> str:
    if seed % 2 == 0:
        return "do_not_continue"
    return "generate_numbers"


def generate_numbers(seed: int) -> List[int]:
    random.seed(seed)
    numbers = [random.randrange(100) for _ in range(TASK_COUNT)]
    return numbers


def process_number(index: int, numbers: List[int]) -> int:
    number = numbers[index]
    if number % 3 == 0:
        raise ValueError(f"Cannot process value: {number}")
    return number**2


def report_numbers(ti) -> None:
    total = 0
    for index in range(TASK_COUNT):
        number = ti.xcom_pull(f"process_number_{index}") or 0
        total += number
    print(f"Total value computed: {total}")
