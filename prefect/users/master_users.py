import prefect
from prefect import task, Flow, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute
import requests
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


RATE_LIMIT_SECONDS = 1.6


def requests_retry_session(
    retries=3,
    backoff_factor=2,
    status_forcelist=(429, 500, 502, 503, 504),
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


@task
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
            f"Error requesting league data, status code{tft_league_call.status_code}"
        )

    tft_league_response = tft_league_call.json()
    time.sleep(RATE_LIMIT_SECONDS)

    return tft_league_response


@task
def update_users(entries, region, tier, rank, password, riot_header, db_args):
    logger = prefect.context.get("logger")
    summonerId = entries["summonerId"]
    summonerName = entries["summonerName"]
    leaguePoints = entries["leaguePoints"]

    puuid_url = (
        f"https://{region}.api.riotgames.com/tft/summoner/v1/summoners/{summonerId}"
    )
    puuid_call = requests_retry_session().get(url=puuid_url, headers=riot_header)

    if puuid_call.status_code != 200:
        logger.warning(
            f"Error requesting puuid data, status code{puuid_call.status_code}"
        )

    puuid_response = puuid_call.json()
    time.sleep(RATE_LIMIT_SECONDS)

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
