from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret
from datetime import timedelta
from prefect.schedules import IntervalSchedule

@task
def my_task(credentials):
    """A task that requires credentials to access something. Passing the
    credentials in as an argument allows you to change how/where the
    credentials are loaded (though we recommend using `PrefectSecret` tasks to
    load them."""
    something = str(credentials)
    print(something)

schedule = IntervalSchedule(interval=timedelta(minutes=1))

with Flow("Hello", schedule) as flow:
    my_secret = PrefectSecret("USERNAME")
    my_task(credentials=my_secret)

flow.run()