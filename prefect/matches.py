from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret
from datetime import timedelta
from prefect.schedules import IntervalSchedule
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
import requests
import time


@task
def get_match_history(username, password, db_name, api_key):
    """A task that requires credentials to access something. Passing the
    credentials in as an argument allows you to change how/where the
    credentials are loaded (though we recommend using `PrefectSecret` tasks to
    load them."""

    riot_header = {"X-Riot-Token": api_key}

    args = {
        "db_name": db_name,
        "user": username,
        "host": "127.0.0.1",
        "port": 5432,
        "commit": True,
    }

    user_query = "SELECT puuid FROM users LIMIT 1"

    all_puuids = PostgresFetch(query=user_query, data=None, fetch="all", **args).run(
        password=password
    )

    for puuid in all_puuids:
        puuid = puuid[0]
        match_id_url = f"https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?count=10"
        recent_match_ids = requests.get(url=match_id_url, headers=riot_header).json()
        time.sleep(1.6)

        match_id_query = "SELECT matchid FROM matchinfo WHERE matchid = ANY(%s);"
        match_id_data = (recent_match_ids,)
        existing_ids = PostgresFetch(
            query=match_id_query, data=match_id_data, fetch="all", **args
        ).run(password=password)

        existing_ids = [i[0] for i in existing_ids]
        new_matches = set(recent_match_ids).symmetric_difference(set(existing_ids))
        new_matches = list(new_matches)
        print(existing_ids)
        print(new_matches)
        for match_id in new_matches:
            match_detail_url = (
                f"https://europe.api.riotgames.com/tft/match/v1/matches/{match_id}"
            )
            time.sleep(1.6)

            match_detail = requests.get(match_detail_url, headers=riot_header).json()

            for participant in match_detail["info"]["participants"]:

                participant_puuid = participant['puuid']

                puuid_query = """
                    SELECT id FROM users WHERE puuid = %s
                    """

                puuid_data = (
                    participant_puuid,
                )

                user_id = PostgresFetch(query=puuid_query, data=puuid_data, fetch="one", **args).run(
                    password=password
                )

                match_query = """
                    INSERT INTO matchinfo VALUES (DEFAULT,%s,%s,%s)
                    """

                match_data = (
                    match_id,
                    user_id,
                    participant["placement"],
                )

                PostgresExecute(query=match_query, data=match_data, **args).run(
                    password=password
                )

                participant_query = """
                    SELECT id FROM matchinfo WHERE userid = %s and matchid = %s
                    """

                participant_data = (user_id, match_id,)

                match_data_id = PostgresFetch(
                    query=participant_query, data=participant_data, fetch="one", **args
                ).run(password=password)

                for units in participant['units']:
                    character_id = units['character_id']
                    tier = units['tier']
                    items = units['items']
                    item1 =  items[0] if len(items)>0 else None
                    item2 = items[1] if len(items)>1 else None
                    item3 = items[1] if len(items)>2 else None

                unit_query = """
                    INSERT INTO matchunits VALUES (%s,%s,%s,%s,%s,%s)
                    """
                unit_data = (match_data_id, character_id, tier, item1, item2, item3,)

                PostgresExecute(query=unit_query, data=unit_data, **args).run(
                    password=password
                )

with Flow("Get Latest Matches") as flow:
    username = PrefectSecret("USERNAME")
    password = PrefectSecret("PASSWORD")
    api_key = PrefectSecret("APIKEY")
    dbname = "teamfighttactics"
    get_match_history(
        username=username, password=password, db_name=dbname, api_key=api_key
    )

flow.run()
