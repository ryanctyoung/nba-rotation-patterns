import matplotlib.pyplot as plt
import pandas as pd
import json
import time
import re
import numpy as np
import requests
from datetime import datetime
from datetime import timedelta
import sys
import platform
import importlib


from nba_api.stats.endpoints import playbyplayv2, playbyplayv3, \
    gamerotation, leaguegamelog, boxscoreadvancedv2, commonteamroster


team_id = 1610612744
season_id = '2023-24'
game_id = '0012300045'
season_type = 'Pre Season'
timeout = 30
retry_attempts = 7
date = ['10/16/2023']


def calculate_time_at_period(current_period):
    if current_period > 5:
        return (720 * 4 + (current_period - 5) * (5 * 60)) * 10
    else:
        return (720 * (current_period - 1)) * 10


def calculate_seconds_elapsed_in_period(current_period, string):
    pt = datetime.strptime(string, '%M:%S')
    total_seconds = pt.second + pt.minute * 60
    return (current_period * 720) - total_seconds

## currently working on scheduler job logic


if __name__ == '__main__':
    dict_list = {}
    pbp = playbyplayv3.PlayByPlayV3(start_period=1, end_period=4, game_id=game_id).get_data_frames()[0]
    scoreHistory = pbp.loc[(pbp.pointsTotal != 0) | (pbp.actionType == 'Substitution'), ['gameId', 'clock', 'period', 'teamId', 'personId', 'playerName', 'scoreHome', 'scoreAway', 'actionType', 'location']]
    scoreHistory.scoreHome = pd.to_numeric(scoreHistory.scoreHome).ffill().astype(np.int64)
    scoreHistory.scoreAway = pd.to_numeric(scoreHistory.scoreAway).ffill().astype(np.int64)
    sub_scoreHistory = scoreHistory.loc[pbp.actionType == 'Substitution']

    def time_conversion(a):
        p = a.period
        time_dict = re.findall(r'\d+', a.clock)
        game_time = 60 * (12 - int(time_dict[0])) + (60 - int(time_dict[1])) + ((p-1)*720)
        return game_time

    sub_scoreHistory['gameTime'] = sub_scoreHistory.apply(
        time_conversion, axis=1)
    result = sub_scoreHistory.drop(['clock', 'period', 'actionType', ], axis=1).to_dict()
    for x in list(result.keys()):
        if x not in dict_list:
            dict_list[x] = []
        dict_list[x].extend(list(result[x].values()))

    df = pd.DataFrame.from_dict(result)
    print(result)
