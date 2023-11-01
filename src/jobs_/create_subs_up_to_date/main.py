from datetime import datetime
import pandas as pd
import numpy as np
import time
import re
import requests

from nba_api.stats.endpoints import gamerotation, leaguegamelog, playbyplayv3
from database.connect import insert_into_db, get_sql_session, df_upsert
from generic.print_to_log.main import print_to_log

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

        game_score_histories = {}
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

        def process_game(game_id, sub_list, score_dict):
            retries = retry_attempts
            print_to_log(game_id)
            while retries > 0:
                try:

                    # game substitution history with score, for Plus Minus
                    pbp = playbyplayv3.PlayByPlayV3(start_period=1, end_period=4, game_id=game_id).get_data_frames()[0]
                    score_history = pbp.loc[
                        (pbp.pointsTotal != 0) | (pbp.actionType == 'Substitution'), [ 'gameId','clock', 'period', 'teamId',
                                                                                      'personId', 'playerName',
                                                                                      'scoreHome', 'scoreAway',
                                                                                      'actionType', 'location']]
                    score_history.scoreHome = pd.to_numeric(score_history.scoreHome).ffill().astype(np.int64)
                    score_history.scoreAway = pd.to_numeric(score_history.scoreAway).ffill().astype(np.int64)
                    sub_score_history = score_history.loc[pbp.actionType == 'Substitution']

                    def time_conversion(a):
                        p = a.period
                        time_dict = re.findall(r'\d+', a.clock)
                        game_time = 60 * (12 - int(time_dict[0])) + (60 - int(time_dict[1])) + ((p - 1) * 720)
                        return game_time

                    game_time = sub_score_history.apply(
                        time_conversion, axis=1)
                    sub_score_history.loc[:, 'gameTime'] = game_time
                    game_score_sub_history = sub_score_history.drop(['clock', 'period', 'actionType', ], axis=1).to_dict()

                    for x in list(game_score_sub_history.keys()):
                        if x not in score_dict:
                            score_dict[x] = []
                        score_dict[x].extend(list(game_score_sub_history[x].values()))

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
                    print_to_log('Request Error')
                    # print_to_log(error)
                    time.sleep(1)
                    retries -= 1
                    if retries == 0:
                        print_to_log('Retries used up!')
                        raise Exception('Too many API timeouts')
                    continue

                break

        list(map(lambda a: process_game(a, roster_subs, game_score_histories), game_series[0:2]))

        subs_df = pd.DataFrame(roster_subs)\

        score_df = pd.DataFrame(game_score_histories)

        session = get_sql_session()
        with session.begin() as conn:
            # game data insert into database
            # insert_into_db(df=game_log, table_name='games', conn=conn)
            df_upsert(df=game_log, table_name='games', conn=conn)

            # subs data insert into database
            # insert_into_db(df=subs_df, table_name='rotations', conn=conn)
            df_upsert(df=subs_df, table_name='rotations', conn=conn)

            # score data insert into database
            df_upsert(df=score_df, table_name='score_histories', conn=conn)
        return True
    except (TimeoutError, ConnectionError) as error:
        print_to_log(error)
        return False


if __name__ == '__main__':
    result = False
    while not result:
        result = create_subs_up_to_date(date='10/23/2023', season_id='2023-24', season_type='Regular Season')
        if not result:
            time.sleep(10)
