import matplotlib.pyplot as plt
import pandas as pd
import json
import numpy as np

from datetime import datetime
from nba_api.stats.endpoints import playbyplayv2, leaguegamefinder, boxscoreadvancedv2, commonteamroster


team_id = 1610612744
game_id = '0022200996'
season_id = '2022-23'


def calculate_time_at_period(current_period):
    if current_period > 5:
        return (720 * 4 + (current_period - 5) * (5 * 60)) * 10
    else:
        return (720 * (current_period - 1)) * 10


def calculate_seconds_elapsed_in_period(current_period, string):
    pt = datetime.strptime(string, '%M:%S')
    total_seconds = pt.second + pt.minute * 60
    return (current_period * 720) - total_seconds


if __name__ == '__main__':

    roster_object = commonteamroster.CommonTeamRoster(season='2022-23', team_id=team_id).get_data_frames()[0]

    roster = roster_object[['PLAYER_ID', 'PLAYER']]
    roster.set_index('PLAYER_ID', inplace=True)
    # roster_subs = roster.apply(lambda a: PlayerSubs(name=a.PLAYER, id=a.name), axis=1).to_dict()
    roster_subs = roster.apply(lambda a: {'id': a.name, 'subs': [], 'name': a.PLAYER}, axis=1).tolist()

    game_finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
    games = game_finder.get_data_frames()[0]
    # game_id = games[games.SEASON_ID == '12023'].iloc[0].GAME_ID

    for period in [1,2,3,4 ]:
        pbp = playbyplayv2.PlayByPlayV2(game_id=game_id, start_period=period, end_period=period) \
            .get_data_frames()[0]
        subs = pbp.loc[(pbp.EVENTMSGTYPE == 8) & (pbp.PLAYER1_TEAM_ID == team_id)]

        bc = boxscoreadvancedv2.BoxScoreAdvancedV2(
            game_id=game_id, end_range=(calculate_time_at_period(period + 1) - 5),
            start_range=(calculate_time_at_period(period) + 5), range_type=2,
        ).get_data_frames()[0]
        players = bc[bc.TEAM_ID == team_id][['PLAYER_NAME', 'PLAYER_ID']]
        players.set_index('PLAYER_ID', inplace=True)

        def init_quarter(row):
            first_sub = subs.loc[(subs.PLAYER1_ID == row.name) | (subs.PLAYER2_ID == row.name)].iloc[0]
            if first_sub.PLAYER1_ID == row.name:
                return [(period - 1) * 720]
            return []

        player_subs_i = players.apply(\
            lambda a: {'id': a.name, 'subs': init_quarter(a)}, axis=1)\
            .tolist()

        def sub_iter(sub):
            next(i for i in player_subs_i if i["id"] == sub.PLAYER1_ID)\
                ['subs'].append(calculate_seconds_elapsed_in_period(period, sub.PCTIMESTRING))
            next(j for j in player_subs_i if j["id"] == sub.PLAYER2_ID)\
                ['subs'].append(calculate_seconds_elapsed_in_period(period, sub.PCTIMESTRING))

        subs.apply(sub_iter, axis=1)

        def end_of_quarter(obj):
            subs_i = obj['subs']
            if bool(len(subs_i) % 2): \
                obj['subs'].append(period * 720)
            return {'id': obj['id'], 'subs': obj['subs']}


        player_subs_i = list(map(end_of_quarter, player_subs_i))

        for p in player_subs_i:
            next(i for i in roster_subs if i["id"] == p['id'])['subs']\
                += p['subs']

    game_df = pd.DataFrame(roster_subs)
    game_df['GAME_ID'] = [game_id]*game_df.shape[0]
    game_json = game_df.to_json()
