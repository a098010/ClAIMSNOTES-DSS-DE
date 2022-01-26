import random

from prefect import Flow, Task, task


@task
def random_number():
    return random.randint(0, 100)


@task
def plus_one(x):
    return x + 1


with Flow("dummy_flow") as flow:
    r = random_number()
    y = plus_one(x=r)

if __name__ == "__main__":
    flow.run()
