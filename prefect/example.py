from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret
from datetime import timedelta
from prefect.schedules import IntervalSchedule
import psycopg2 as pg
import requests
import pandas as pd

@task
def my_task(username, password, db_name, api_key):
    """A task that requires credentials to access something. Passing the
    credentials in as an argument allows you to change how/where the
    credentials are loaded (though we recommend using `PrefectSecret` tasks to
    load them."""
    challenger_url = f'https://euw1.api.riotgames.com/tft/league/v1/challenger?api_key={api_key}'
    challenger_response = requests.get(url = challenger_url).json()
    challenger_response_df = pd.DataFrame(challenger_response['entries'])


    for i in range(0, challenger_response_df.shape[0]):
        summonerId = challenger_response_df["summonerId"][i]
        summonerName = challenger_response_df["summonerName"][i]
        leaguePoints = challenger_response_df["leaguePoints"][i]

        conn = pg.connect(
            dbname=db_name,
            user=username,
            password=password,
            host = '127.0.0.1'
        )

        query = f'''
            INSERT INTO public."Users"(
                id, "summonerName", "summonerId", "rankedLeague", puuid, leaguepoints, region)
                VALUES (DEFAULT, '{summonerName}', '{summonerId}', 'CHALLENGER', 'Temporary', {leaguePoints} , 'EUW1')
                '''

        with conn.cursor() as cursor:
            cursor.execute(query=query)
            conn.commit()

        # ensure connection is closed
        conn.close()


schedule = IntervalSchedule(interval=timedelta(minutes=60))

with Flow("Hello", schedule) as flow:
    username = PrefectSecret("USERNAME")
    password = PrefectSecret("PASSWORD")
    api_key = PrefectSecret("APIKEY")
    dbname= 'teamfighttactics' 
    my_task(username = username, password = password,db_name = dbname, api_key = api_key)

flow.run()