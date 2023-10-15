import matplotlib.pyplot as plt
import pandas as pd
import json
import numpy as np

from datetime import datetime
from nba_api.stats.endpoints import playbyplayv2,\
    playercareerstats,\
    gamerotation, \
    leaguegamelog, boxscoreadvancedv2, commonteamroster

team_id = 1610612744
game_id = '0022200002'
season_id = '2022-23'
timeout = 20

if __name__ == '__main__':
    game_log = leaguegamelog.LeagueGameLog(
        timeout=timeout,
        season=season_id,
        player_or_team_abbreviation='T',
        season_type_all_star='Regular Season').get_data_frames()[0]

    game_ids = game_log.loc[game_log.TEAM_ID == team_id].loc[:, "GAME_ID"].tolist()

    print(game_ids)