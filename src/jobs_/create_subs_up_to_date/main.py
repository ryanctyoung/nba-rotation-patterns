from datetime import datetime
import pandas as pd
import time

import requests

from nba_api.stats.endpoints import gamerotation, leaguegamelog
from database.connect import insert_into_db, get_sql_session

timeout = 10
retry_attempts = 7


def calculate_time_at_period(current_period):
    if current_period > 5:
        return (720 * 4 + (current_period - 5) * (5 * 60)) * 10
    else:
        return (720 * (current_period - 1)) * 10


def calculate_seconds_elapsed_in_period(current_period, string):
    pt = datetime.strptime(string, '%M:%S')
    total_seconds = pt.second + pt.minute * 60
    return (current_period * 720) - total_seconds


def create_subs_up_to_date(
        date,
        season_id,
        season_type='Regular Season',
        ):
    try:

        roster_subs = []

        game_log = leaguegamelog.LeagueGameLog(
            timeout=timeout,
            season=season_id,
            player_or_team_abbreviation='T',
            season_type_all_star=season_type,
            date_from_nullable=[date]
            ).get_data_frames()[0]
        time.sleep(2)
        game_series = game_log.loc[:, "GAME_ID"].unique()


        def process_game(game_id, sub_list):
            retries = retry_attempts
            print(game_id)
            while retries > 0:
                try:
                    game_rosters = gamerotation.GameRotation(
                        timeout=timeout,
                        game_id=game_id,
                        league_id=0,
                    ).get_data_frames()

                    team_ids = [
                        game_rosters[0].loc[0, 'TEAM_ID'],
                        game_rosters[1].loc[0, 'TEAM_ID']
                    ]

                    for team_id in team_ids:
                        roster_obj = \
                            next(x for x in game_rosters if team_id in x.loc[:1, 'TEAM_ID'].values) \
                                .loc[:, ['PERSON_ID', 'PLAYER_FIRST', 'PLAYER_LAST', 'IN_TIME_REAL', 'OUT_TIME_REAL']]

                        players = roster_obj.loc[:, ['PERSON_ID', 'PLAYER_FIRST', 'PLAYER_LAST',]].drop_duplicates()
                        roster_subs_per_game = players.apply(
                            lambda a: {'PLAYER_ID': a.PERSON_ID,
                                       'SUBS': list(map(int,
                                                        pd.Series(roster_obj.loc[roster_obj.PERSON_ID == a.PERSON_ID, ['IN_TIME_REAL', 'OUT_TIME_REAL']]
                                                                  .values.tolist()).explode().tolist())),
                                       'PLAYER_NAME': (a.PLAYER_FIRST + " " + a.PLAYER_LAST),
                                       'GAME_ID': game_id},
                            axis=1).tolist()
                        sub_list += roster_subs_per_game
                        retries = retry_attempts
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as error:
                    print('Request Error')
                    # print(error)
                    time.sleep(1)
                    retries -= 1
                    if retries == 0:
                        print('Retries used up!')
                        raise Exception('Too many API timeouts')
                    continue

                break

        list(map(lambda a: process_game(a, roster_subs), game_series))

        subs_df = pd.DataFrame(roster_subs)

        session = get_sql_session()
        with session.begin() as conn:
            # game data insert into database
            insert_into_db(df=game_log, table_name='games', conn=conn)

            # subs data insert into database
            insert_into_db(df=subs_df, table_name='rotations', conn=conn)
        return True
    except Exception as error:
        print(error)
        return False


if __name__ == '__main__':
    result = False
    while not result:
        result = create_subs_up_to_date(date='10/20/2023', season_id='2023-24', season_type='Pre Season')
        if not result:
            time.sleep(10)
