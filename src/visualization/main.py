import json

import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

seconds_per_game = 60 * 12 * 4
seconds_per_period = int(seconds_per_game / 4)

def string_to_int_list(string):
    if string == '[]':
        return []
    subs_strings = string.strip('][').split(', ')
    return list(map(lambda a: int(a), subs_strings))


if __name__ == '__main__':
    data = pd.read_csv('../../data/test.csv')
    players = data['PLAYER_NAME'].unique()
    time_axis = np.zeros(seconds_per_game, dtype=int)
    player_dict = dict(zip(players, np.zeros((players.size, seconds_per_game), dtype=int)))

    fig = make_subplots(
        rows=len(players), cols=1,
        vertical_spacing=0.03,
        row_titles=list(players)
    )

    for i, player_name in enumerate(players):
        games = data.loc[data.PLAYER_NAME == player_name]
        no_of_games = games.shape[0]

        def iter_through_game(game):
            subs = string_to_int_list(game['SUBS'])
            i = 0
            while i < len(subs) - 1:
                for j in range(subs[i], subs[i + 1]):
                    player_dict[player_name][j] += 1
                i += 2

        games.apply(iter_through_game, axis=1)
        averaged_log = player_dict[player_name] / no_of_games

        fig.add_trace(
            go.Scatter(
                x=list(range(0, time_axis.size)),
                y=averaged_log,
                name=player_name,
                text="Over " + str(no_of_games) + " games"
            ),
            row=i+1, col=1,
        )

    fig.update_xaxes(
        range=[0, seconds_per_game],
        tickmode='array',
        tickvals=[0, seconds_per_period * 1, seconds_per_period * 2, seconds_per_period * 3],
        ticktext=['1st', '2nd', '3rd', '4th'])

    fig.update_yaxes(
        range=[0, 1],
        tickmode='array',
        tickvals=[],
        ticktext=[])

    fig.update_layout(
        height=3000, width=1400,
        title_text="GSW 2022-23 Rotation Patterns",
        paper_bgcolor="LightSteelBlue",
    )
    fig.show()

