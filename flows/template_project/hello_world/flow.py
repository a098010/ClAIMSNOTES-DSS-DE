from prefect import Flow, Task, task


@task
def hello_world():
    print("Hello World!")


with Flow("hello_world") as flow:
    r = hello_world()

if __name__ == "__main__":
    flow.run()
