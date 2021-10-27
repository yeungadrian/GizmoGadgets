from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute
import psycopg2 as pg
import requests
import pandas as pd
import time

@task
def update_challenger_league(username, password, db_name, api_key):
    """A task that requires credentials to access something. Passing the
    credentials in as an argument allows you to change how/where the
    credentials are loaded (though we recommend using `PrefectSecret` tasks to
    load them."""
    query_header = {
            "X-Riot-Token": api_key
    }
    challenger_url = 'https://euw1.api.riotgames.com/tft/league/v1/challenger'
    challenger_response = requests.get(url = challenger_url, headers=query_header).json()
    challenger_response_df = pd.DataFrame(challenger_response['entries'])

    for i in range(0, challenger_response_df.shape[0]):
        summonerId = str(challenger_response_df["summonerId"][i])
        summonerName = str(challenger_response_df["summonerName"][i])
        leaguePoints = int(challenger_response_df["leaguePoints"][i])

        puuid_url = f'https://euw1.api.riotgames.com/tft/summoner/v1/summoners/{summonerId}'
        puuid_response = requests.get(url = puuid_url, headers=query_header).json()
        puuid = str(puuid_response["puuid"])

        time.sleep(1.6)

        user_query = '''
            INSERT INTO public.users(
                id, summonername, summonerid, rankedleague, puuid, leaguepoints, region)
                VALUES (DEFAULT, %s, %s, 'CHALLENGER', %s, %s , 'EUW1')
                '''
        query_data = (summonerName, summonerId, puuid,leaguePoints,)

        PostgresExecute(
            db_name=db_name,
            user=username,
            host="127.0.0.1",
            port=5432,
            query=user_query,
            data=query_data,
            commit=True,
        ).run(password=password)


with Flow("Update Challenger League") as flow:
    username = PrefectSecret("USERNAME")
    password = PrefectSecret("PASSWORD")
    api_key = PrefectSecret("APIKEY")
    dbname= 'teamfighttactics' 
    update_challenger_league(username = username, password = password,db_name = dbname, api_key = api_key)

flow.run()