from prefect import task, Flow, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
import requests
import time

@task
def get_grandmaster_users(password, db_args):

    user_query = """ SELECT puuid FROM users WHERE rankedleague = 'GRANDMASTER' """

    all_puuids = PostgresFetch(query=user_query, data=None, fetch="all", **db_args).run(
        password=password
    )

    return all_puuids

@task
def update_recent_match_history(puuid, password, riot_header, db_args):
    match_id_url = f"https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?count=5"
    recent_match_ids = requests.get(url=match_id_url, headers=riot_header).json()
    time.sleep(1.6)

    match_id_query = "SELECT matchid FROM matchinfo WHERE matchid = ANY(%s);"
    match_id_data = (recent_match_ids,)
    existing_ids = PostgresFetch(
        query=match_id_query, data=match_id_data, fetch="all", **db_args
    ).run(password=password)

    existing_ids = [i[0] for i in existing_ids]
    new_matches = set(recent_match_ids).symmetric_difference(set(existing_ids))
    new_matches = list(new_matches)

    for match_id in new_matches:
        match_detail_url = (
            f"https://europe.api.riotgames.com/tft/match/v1/matches/{match_id}"
        )
        time.sleep(1.6)

        match_detail = requests.get(match_detail_url, headers=riot_header).json()

        for participant in match_detail["info"]["participants"]:

            user_id = participant["puuid"]

            match_query = """
                INSERT INTO matchinfo VALUES (DEFAULT,%s,%s,%s)
                """

            match_data = (
                match_id,
                user_id,
                participant["placement"],
            )

            PostgresExecute(query=match_query, data=match_data, **db_args).run(
                password=password
            )

            participant_query = """
                SELECT id FROM matchinfo WHERE userid = %s and matchid = %s
                """

            participant_data = (
                user_id,
                match_id,
            )

            match_data_id = PostgresFetch(
                query=participant_query, data=participant_data, fetch="one", **db_args
            ).run(password=password)

            for units in participant["units"]:
                character_id = units["character_id"]
                tier = units["tier"]
                items = units["items"]
                item1 = items[0] if len(items) > 0 else None
                item2 = items[1] if len(items) > 1 else None
                item3 = items[1] if len(items) > 2 else None

            unit_query = """
                INSERT INTO matchunits VALUES (%s,%s,%s,%s,%s,%s)
                """
            unit_data = (
                match_data_id,
                character_id,
                tier,
                item1,
                item2,
                item3,
            )

            PostgresExecute(query=unit_query, data=unit_data, **db_args).run(
                password=password
            )

with Flow("Get Latest Grandmaster Matches") as flow:
    password = PrefectSecret("PASSWORD")

    riot_header = {"X-Riot-Token": PrefectSecret("APIKEY")}

    db_args = {
        "db_name": "teamfighttactics",
        "user": PrefectSecret("USERNAME"),
        "host": "127.0.0.1",
        "port": 5432,
        "commit": True,
    }

    all_puuids = get_grandmaster_users(
         password=password, db_args = db_args
    )
    
    update_recent_match_history.map(
        puuid=all_puuids, password=unmapped(password), riot_header=unmapped(riot_header), db_args=unmapped(db_args)
    )