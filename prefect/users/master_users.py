import prefect
from prefect import task, Flow, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
from pydantic import BaseModel
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time

RATE_LIMIT_SECONDS = 1.6

class RiotUser(BaseModel):
    summonerId: str
    summonerName: str
    leaguePoints: int
    rank: str


def requests_retry_session(
    retries=3,
    backoff_factor=2,
    status_forcelist=(429, 500, 503),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def get_users_from_league(region, league, rank, riot_header):
    logger = prefect.context.get("logger")

    if league in ["challenger", "grandmaster", "master"]:
        tft_league_url = f"https://{region}.api.riotgames.com/tft/league/v1/{league}"
    else:
        tft_league_url = (
            f"https://{region}.api.riotgames.com/tft/league/v1/entries/{league}/{rank}"
        )
    tft_league_call = requests_retry_session().get(
        url=tft_league_url, headers=riot_header
    )

    if tft_league_call.status_code != 200:
        logger.warning(
            f"Error calling riotgames api for league, status code {tft_league_call.status_code}"
        )

    tft_league_response = tft_league_call.json()
    time.sleep(RATE_LIMIT_SECONDS)

    return tft_league_response


def update_users(user, region, tier, rank, password, riot_header, db_args):
    logger = prefect.context.get("logger")
    riot_user = RiotUser(**user)

    summoner_query = """
        SELECT id FROM users WHERE summonerId = %s
    """

    summoner_data = (riot_user.summonerId,)

    summoner = PostgresFetch(
        query=summoner_query, data=summoner_data, fetch="all", **db_args
    ).run(password=password)

    if summoner == None:

        puuid_url = f"https://{region}.api.riotgames.com/tft/summoner/v1/summoners/{riot_user.summonerId}"
        puuid_call = requests_retry_session().get(url=puuid_url, headers=riot_header)

        if puuid_call.status_code != 200:
            logger.warning(
                f"Error calling riotgames api for puuid, status code{puuid_call.status_code}"
            )

        puuid_response = puuid_call.json()
        time.sleep(RATE_LIMIT_SECONDS)

        puuid = str(puuid_response["puuid"])

        user_query = """
            INSERT INTO users(
                id, summonername, summonerid, league, ranktier, puuid, leaguepoints, region)
            VALUES (DEFAULT, %s, %s, %s,%s, %s, %s, %s)
            ON CONFLICT (summonerid)
            DO UPDATE SET (league, leaguepoints)= (EXCLUDED.league, EXCLUDED.leaguepoints);
            """

        query_data = (
            riot_user.summonerName,
            riot_user.summonerId,
            tier,
            rank,
            puuid,
            riot_user.leaguePoints,
            region,
        )

    else:
        user_query = """
            UPDATE users
            set league = %s,
            ranktier = %s,
            leaguepoints = %s
            WHERE summonerid = %s
            """

        query_data = (
            tier,
            rank,
            riot_user.leaguePoints,
            riot_user.summonerId,
        )

    PostgresExecute(query=user_query, data=query_data, **db_args).run(password=password)


@task
def users_flow(region, divisions, league, password, riot_header, db_args):
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

        for user in users:
            update_users(
                user=user,
                region=region,
                tier=tier,
                rank=rank,
                password=password,
                riot_header=riot_header,
                db_args=db_args,
            )


with Flow("Summoner - Master") as flow:
    password = PrefectSecret("PASSWORD")
    league = "master"
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

    users_flow.map(
        region=regions,
        divisions=unmapped(divisions),
        league=unmapped(league),
        password=unmapped(password),
        riot_header=unmapped(riot_header),
        db_args=unmapped(db_args),
    )
