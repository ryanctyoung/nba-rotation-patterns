import matplotlib.pyplot as plt
import pandas as pd
import json
import time
import numpy as np
import requests
from datetime import datetime
from datetime import timedelta

from nba_api.stats.endpoints import playbyplayv2,\
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
    game_rosters = gamerotation.GameRotation(
        timeout=timeout,
        game_id=game_id,
        league_id=0,
    ).get_data_frames()
    print(game_rosters)