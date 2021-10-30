from prefect import task, Flow, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute
import requests
import time


@task
def get_users_from_league(league, riot_header):
    league_url = f"https://euw1.api.riotgames.com/tft/league/v1/{league}"
    league_response = requests.get(url=league_url, headers=riot_header).json()
    time.sleep(2)

    return league_response


@task
def update_users(entries, tier, password, riot_header, db_args):


    summonerId = entries["summonerId"]
    summonerName = entries["summonerName"]
    leaguePoints = entries["leaguePoints"]

    puuid_url = (
        f"https://euw1.api.riotgames.com/tft/summoner/v1/summoners/{summonerId}"
    )
    puuid_response = requests.get(url=puuid_url, headers=riot_header).json()
    time.sleep(2)
    puuid = str(puuid_response["puuid"])

    user_query = """
        INSERT INTO public.users(
            id, summonername, summonerid, rankedleague, puuid, leaguepoints, region)
        VALUES (DEFAULT, %s, %s, %s, %s, %s , 'EUW1')
        ON CONFLICT (summonerid)
        DO UPDATE SET (rankedleague, leaguepoints)= (EXCLUDED.rankedleague, EXCLUDED.leaguepoints);
        """

    query_data = (
        summonerName,
        summonerId,
        tier,
        puuid,
        leaguePoints,
    )

    PostgresExecute(query=user_query, data=query_data, **db_args).run(
        password=password
    )


with Flow("Get Challenger Summoners") as flow:
    password = PrefectSecret("PASSWORD")

    riot_header = {"X-Riot-Token": PrefectSecret("APIKEY")}

    db_args = {
        "db_name": "teamfighttactics",
        "user": PrefectSecret("USERNAME"),
        "host": "127.0.0.1",
        "port": 5432,
        "commit": True,
    }

    user_response = get_users_from_league(
        league="challenger",
        riot_header=riot_header,
    )
    tier = user_response["tier"]
    users = user_response["entries"]

    update_users.map(entries=users, tier = unmapped(tier), password=unmapped(password),riot_header=unmapped(riot_header), db_args=unmapped(db_args))
