from prefect import task, Flow
from datetime import timedelta
from prefect.schedules import IntervalSchedule

@task
def say_hello():
    print("Hello, world!")

schedule = IntervalSchedule(interval=timedelta(minutes=1))

with Flow("Hello", schedule) as flow:
    say_hello()

flow.run()