from datetime import datetime
import pandas as pd
import time
import requests

from nba_api.stats.endpoints import playbyplayv2,\
    gamerotation, leaguegamelog, boxscoreadvancedv2

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
                    time.sleep(2)

                    game_pbp = playbyplayv2.PlayByPlayV2(
                            timeout=timeout,
                            game_id=game_id).get_data_frames()[0]
                    time.sleep(2)

                    game_box_score = boxscoreadvancedv2.BoxScoreAdvancedV2(
                            timeout=timeout,
                            game_id=game_id,
                        ).get_data_frames()[0]
                    time.sleep(2)

                    team_ids = [
                        game_rosters[0].loc[0, 'TEAM_ID'],
                        game_rosters[1].loc[0, 'TEAM_ID']
                    ]

                    for team_id in team_ids:
                        roster = \
                            next(x for x in game_rosters if team_id in x.loc[:1, 'TEAM_ID'].values) \
                                .loc[:, ['PERSON_ID', 'PLAYER_FIRST', 'PLAYER_LAST']].drop_duplicates()

                        roster_subs_per_game = roster.apply(
                            lambda a: {'PLAYER_ID': a.PERSON_ID,
                                       'SUBS': [], 'PLAYER_NAME': (a.PLAYER_FIRST + " " + a.PLAYER_LAST),
                                       'GAME_ID': game_id},
                            axis=1).tolist()

                        game_subs = game_pbp.loc[(game_pbp.EVENTMSGTYPE == 8) & (game_pbp.PLAYER1_TEAM_ID == team_id)]

                        players = game_box_score[game_box_score.TEAM_ID == team_id][['PLAYER_NAME', 'PLAYER_ID']]
                        players.set_index('PLAYER_ID', inplace=True)

                        for period in [1, 2, 3, 4]:

                            subs_this_period = game_subs.loc[(game_pbp.PERIOD == period)]

                            def init_quarter(row):
                                total_subs = subs_this_period.loc[
                                    (subs_this_period.PLAYER1_ID == row.name) | (subs_this_period.PLAYER2_ID == row.name)]
                                if len(total_subs) == 0:
                                    return []

                                first_sub = total_subs.iloc[0]
                                if first_sub.PLAYER1_ID == row.name:
                                    return [(period - 1) * 720]
                                return []

                            player_subs_i = players.apply(
                                lambda a: {'PLAYER_ID': a.name, 'SUBS': init_quarter(a)}, axis=1) \
                                .tolist()

                            def sub_iter(sub):
                                next(i for i in player_subs_i if i['PLAYER_ID'] == sub.PLAYER1_ID) \
                                    ['SUBS'].append(calculate_seconds_elapsed_in_period(period, sub.PCTIMESTRING))
                                next(j for j in player_subs_i if j['PLAYER_ID'] == sub.PLAYER2_ID) \
                                    ['SUBS'].append(calculate_seconds_elapsed_in_period(period, sub.PCTIMESTRING))

                            subs_this_period.apply(sub_iter, axis=1)

                            def end_of_quarter(obj):
                                subs_i = obj['SUBS']
                                if bool(len(subs_i) % 2): \
                                        obj['SUBS'].append(period * 720)
                                return {'PLAYER_ID': obj['PLAYER_ID'], 'SUBS': obj['SUBS']}

                            player_subs_i = list(map(end_of_quarter, player_subs_i))

                            for p in player_subs_i:
                                player_i = next((i for i in roster_subs_per_game if i['PLAYER_ID'] == p['PLAYER_ID']), None)
                                if player_i != None:
                                    player_i['SUBS'] += p['SUBS']
                            retries = retry_attempts
                        break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as error:
                    print('Request Error')
                    # print(error)
                    time.sleep(1)
                    retries -= 1
                    if retries == 0:
                        print('Retries used up!')
                        raise Exception('Too many API timeouts')
                    continue
                sub_list += roster_subs_per_game

        list(map(lambda a: process_game(a, roster_subs), game_series[0:2]))


        game_df = pd.DataFrame(roster_subs)
        game_df.to_csv('../../../data/schedule_test.json')
        return True
    except Exception as error:
        return False


if __name__ == '__main__':
    result = False
    while not result:
        result = create_subs_up_to_date(date='10/16/2023', season_id='2023-24', season_type='Pre Season')
        time.sleep(5)
