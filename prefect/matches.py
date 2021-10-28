from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret
from datetime import timedelta
from prefect.schedules import IntervalSchedule
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
import requests
import time
import datetime


@task
def get_match_history(username, password, db_name, api_key):
    """A task that requires credentials to access something. Passing the
    credentials in as an argument allows you to change how/where the
    credentials are loaded (though we recommend using `PrefectSecret` tasks to
    load them."""

    riot_header = {"X-Riot-Token": api_key}

    user_query = "SELECT puuid FROM users"

    all_puuids = PostgresFetch(
        db_name=db_name,
        user=username,
        host="127.0.0.1",
        port=5432,
        query=user_query,
        data=None,
        fetch="all",
        commit=True,
    ).run(password=password)

    for puuid in all_puuids:
        puuid = puuid[0]
        match_id_url = f"https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?count=20"
        recent_match_ids = requests.get(url=match_id_url, headers=riot_header).json()
        time.sleep(1.6)

        match_id_query = "SELECT matchid FROM matchdata WHERE matchid = ANY(%s);"
        match_id_data = (recent_match_ids,)
        existing_ids = PostgresFetch(
            db_name=dbname,
            user=username,
            host="127.0.0.1",
            port=5432,
            query=match_id_query,
            data=match_id_data,
            fetch="all",
            commit=True,
        ).run(password=password)

        existing_ids = [i[0] for i in existing_ids]
        new_matches = set(recent_match_ids).symmetric_difference(set(existing_ids))
        new_matches = list(new_matches)

        for match_id in new_matches:
            match_detail_url = (
                f"https://europe.api.riotgames.com/tft/match/v1/matches/{j}"
            )
            time.sleep(1.6)
            match_detail = requests.get(match_detail_url, headers=riot_header).json()

            match_time = datetime.datetime.fromtimestamp(
                match_detail["info"]["game_datetime"] / 1e3
            ).strftime("%Y-%m-%d %H:%M:%S")
            time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            match_query = """
                    INSERT INTO matchdata VALUES (DEFAULT, %s,%s,%s, 'EUW')
                    """

            match_data = (
                match_id,
                str(match_time),
                str(time_now),
            )

            PostgresExecute(
                db_name=dbname,
                user=username,
                host="127.0.0.1",
                port=5432,
                query=match_query,
                data=match_data,
                commit=True,
            ).run(password=password)

            '''
                We have all the match detail
                1. Insert into matchdata [done]
                Get id we just inserted
                Loop for each participant
                    2. Insert into matchuserinfo
                    Get id we just inserteds
                    3. Insert into matchtraits
                    4. Insert into matchunits
                    Need to split this into functions, although always passing db_name, etc is a pain
            '''


with Flow("Get Latest Matches") as flow:
    username = PrefectSecret("USERNAME")
    password = PrefectSecret("PASSWORD")
    api_key = PrefectSecret("APIKEY")
    dbname = "teamfighttactics"
    get_match_history(
        username=username, password=password, db_name=dbname, api_key=api_key
    )

flow.run()
