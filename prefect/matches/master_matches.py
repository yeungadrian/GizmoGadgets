import prefect
from prefect import task, Flow, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from typing import List
from pydantic import BaseModel
import datetime


RATE_LIMIT_SECONDS = 2


class TFTUnits(BaseModel):
    character_id: str
    items: List[int]
    tier: int


class TFTTraits(BaseModel):
    name: str
    num_units: int
    tier_current: int


def requests_retry_session(
    retries=5,
    backoff_factor=5,
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


def get_users_from_league(region, league, password, db_args):

    user_query = "SELECT puuid FROM users WHERE league = %s and region = %s"
    user_data = (league, region)
    all_puuids = PostgresFetch(
        query=user_query, data=user_data, fetch="all", **db_args
    ).run(password=password)

    return all_puuids


def update_recent_match_history(region, puuid, logger, password, riot_header, db_args):

    if region in ["EUW1", "EUN1", "RU", "TR1"]:
        route = "europe"
    elif region in ["JP1", "KR"]:
        route = "asia"
    else:
        route = "americas"

    match_id_url = f"https://{route}.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?count=5"

    recent_match_ids_call = requests_retry_session().get(
        url=match_id_url, headers=riot_header
    )

    if recent_match_ids_call.status_code != 200:
        logger.warning(
            f"Error requesting puuid data, status code{recent_match_ids_call.status_code}"
        )
    recent_match_ids = recent_match_ids_call.json()
    time.sleep(RATE_LIMIT_SECONDS)

    match_id_query = "SELECT DISTINCT matchid FROM matchinfo WHERE matchid = ANY(%s);"
    match_id_data = (recent_match_ids,)
    existing_ids = PostgresFetch(
        query=match_id_query, data=match_id_data, fetch="all", **db_args
    ).run(password=password)

    existing_ids = [i[0] for i in existing_ids]
    new_matches = set(recent_match_ids).symmetric_difference(set(existing_ids))
    new_matches = list(new_matches)

    for match_id in new_matches:
        update_match(
            match_id=match_id,
            route=route,
            logger=logger,
            password=password,
            riot_header=riot_header,
            db_args=db_args,
        )


def update_participant_matchinfo(
    user_id, match_id, participant, match_time, password, db_args
):
    match_query = """
            INSERT INTO matchinfo (id, matchid, puuid, matchdate, updateddate, placement) VALUES (DEFAULT,%s,%s,%s,DEFAULT,%s)
            """

    match_data = (
        match_id,
        user_id,
        match_time,
        participant["placement"],
    )

    PostgresExecute(query=match_query, data=match_data, **db_args).run(
        password=password
    )


def get_matchinfo_id(user_id, match_id, password, db_args):
    participant_query = """
    SELECT id FROM matchinfo WHERE puuid = %s and matchid = %s
    """

    participant_data = (
        user_id,
        match_id,
    )

    match_data_id = PostgresFetch(
        query=participant_query,
        data=participant_data,
        fetch="one",
        **db_args,
    ).run(password=password)

    return match_data_id


def update_match(match_id, route, logger, password, riot_header, db_args):
    match_detail_url = (
        f"https://{route}.api.riotgames.com/tft/match/v1/matches/{match_id}"
    )

    match_detail_call = requests_retry_session().get(
        match_detail_url, headers=riot_header
    )

    if match_detail_call.status_code != 200:
        logger.warning(
            f"Error requesting puuid data, status code{match_detail_call.status_code}"
        )
    match_detail = match_detail_call.json()

    time.sleep(RATE_LIMIT_SECONDS)

    match_time = match_detail["info"]["game_datetime"]

    match_time = datetime.datetime.fromtimestamp(match_time / 1e3)

    for participant in match_detail["info"]["participants"]:
        user_id = participant["puuid"]
        update_participant_matchinfo(
            user_id, match_id, participant, match_time, password, db_args
        )

        match_data_id = get_matchinfo_id(user_id, match_id, password, db_args)

        for units in participant["units"]:
            update_match_units(
                units=TFTUnits(**units),
                match_data_id=match_data_id,
                password=password,
                db_args=db_args,
            )

        for traits in participant["traits"]:
            update_match_traits(
                traits=TFTTraits(**traits),
                match_data_id=match_data_id,
                password=password,
                db_args=db_args,
            )


def update_match_units(units, match_data_id, password, db_args):
    items = units.items
    item1 = items[0] if len(items) > 0 else None
    item2 = items[1] if len(items) > 1 else None
    item3 = items[1] if len(items) > 2 else None

    unit_query = """
        INSERT INTO matchunits (matchinfoid, unitname, unitstar, item1, item2, item3) VALUES (%s,%s,%s,%s,%s,%s)
        """
    unit_data = (
        match_data_id,
        units.character_id,
        units.tier,
        item1,
        item2,
        item3,
    )

    PostgresExecute(query=unit_query, data=unit_data, **db_args).run(password=password)


def update_match_traits(traits, match_data_id, password, db_args):
    unit_query = """
        INSERT INTO matchtraits (matchinfoid, name, numberunits, tier) VALUES (%s,%s,%s,%s)
        """
    unit_data = (
        match_data_id,
        traits.name,
        traits.num_units,
        traits.tier_current,
    )

    PostgresExecute(query=unit_query, data=unit_data, **db_args).run(password=password)


@task
def update_match_flow(region, league, password, riot_header, db_args):
    logger = prefect.context.get("logger")
    user_puuids = get_users_from_league(
        region=region, league=league, password=password, db_args=db_args
    )

    for puuid in user_puuids:
        update_recent_match_history(
            region=region,
            puuid=puuid,
            logger=logger,
            password=password,
            riot_header=riot_header,
            db_args=db_args,
        )


with Flow("Matches - Master") as flow:
    password = PrefectSecret("PASSWORD")
    league = "MASTER"
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

    update_match_flow.map(
        region=regions,
        league=unmapped(league),
        password=unmapped(password),
        riot_header=unmapped(riot_header),
        db_args=unmapped(db_args),
    )
