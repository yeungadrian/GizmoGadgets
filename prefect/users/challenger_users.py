from prefect import task, Flow, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute
import requests
import time


@task
def get_users_from_league(region, league, rank, riot_header):
    if league in ["challenger", "grandmaster", "master"]:
        tft_league_url = f"https://{region}.api.riotgames.com/tft/league/v1/{league}"
    else:
        tft_league_url = (
            f"https://{region}.api.riotgames.com/tft/league/v1/entries/{league}/{rank}"
        )
    tft_league_response = requests.get(url=tft_league_url, headers=riot_header).json()
    time.sleep(3)

    return tft_league_response


@task
def update_users(entries, region, tier, rank, password, riot_header, db_args):

    summonerId = entries["summonerId"]
    summonerName = entries["summonerName"]
    leaguePoints = entries["leaguePoints"]

    puuid_url = (
        f"https://{region}.api.riotgames.com/tft/summoner/v1/summoners/{summonerId}"
    )
    puuid_response = requests.get(url=puuid_url, headers=riot_header).json()

    time.sleep(3)

    puuid = str(puuid_response["puuid"])

    user_query = """
        INSERT INTO public.users(
            id, summonername, summonerid, league, ranktier, puuid, leaguepoints, region)
        VALUES (DEFAULT, %s, %s, %s,%s, %s, %s, %s)
        ON CONFLICT (summonerid)
        DO UPDATE SET (league, leaguepoints)= (EXCLUDED.league, EXCLUDED.leaguepoints);
        """

    query_data = (
        summonerName,
        summonerId,
        tier,
        rank,
        puuid,
        leaguePoints,
        region,
    )

    PostgresExecute(query=user_query, data=query_data, **db_args).run(password=password)


with Flow("Summoner - Challenger") as flow:
    password = PrefectSecret("PASSWORD")
    league = "challenger"

    riot_header = {"X-Riot-Token": PrefectSecret("APIKEY")}

    db_args = {
        "db_name": "teamfighttactics",
        "user": PrefectSecret("USERNAME"),
        "host": "127.0.0.1",
        "port": 5432,
        "commit": True,
    }

    regions = [
        "BR1",
        "EUN1",
        "EUW1",
        "JP1",
        "KR",
        "LA1",
        "LA2",
        "NA1",
        "OC1",
        "TR1",
        "RU",
    ]

    divisions = ["I"]

    for region in regions:
        for rank in divisions:
            user_response = get_users_from_league(
                region=region,
                league=league,
                rank=rank,
                riot_header=riot_header,
            )

            if league in ["challenger", "grandmaster", "master"]:
                tier = user_response["tier"]
                users = user_response["entries"]
            else:
                tier = league
                users = user_response

            update_users.map(
                entries=users,
                region=unmapped(region),
                tier=unmapped(tier),
                rank=unmapped(rank),
                password=unmapped(password),
                riot_header=unmapped(riot_header),
                db_args=unmapped(db_args),
            )