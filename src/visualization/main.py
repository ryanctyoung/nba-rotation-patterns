import json
from dash import Dash, html, dcc, callback, Output, Input

import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
from database.select_queries import select_rotations_by_game, select_scores_by_game, select_games_by_id
from database.connect import get_sql_session

seconds_per_game = 60 * 12 * 4
seconds_per_period = int(seconds_per_game / 4)
game_id = "0022300061"


def string_to_int_list(string):
    if string == '[]':
        return []
    subs_strings = string.strip('}{').split(',')
    return list(map(lambda a: int(int(a)/10), subs_strings))

session = get_sql_session()
with session.connect() as conn:
    rotations = select_rotations_by_game(conn, game_id)
    score_history = select_scores_by_game(conn, game_id)
    game_boxes = select_games_by_id(conn, game_id)

home_team_id = game_boxes.iloc[0]['TEAM_ID'] if 'vs. ' in game_boxes.iloc[0]['MATCHUP'] else game_boxes.iloc[1]['TEAM_ID']

players = rotations['PLAYER_NAME'].unique()
time_axis = np.zeros(seconds_per_game, dtype=int)
player_dict = dict(zip(players, np.zeros((players.size, seconds_per_game), dtype=int)))

@callback(
    Output('graph-content', 'figure'),
    Input('player_name', 'value')
)
def update_output(player_name):
    selected_player = rotations[rotations['PLAYER_NAME'] == player_name].iloc[0]

    subs = string_to_int_list(selected_player['SUBS'])

    i = 0
    while i < len(subs) - 1:
        for j in range(subs[i], subs[i + 1]):
            player_dict[player_name][j] += 1
        i += 2

    # is_home_team = score_history.loc[score_history['PLAYER_NAME'] == player_name]['TEAM_ID'] == home_team_id

    is_home_team = True

    if is_home_team:
        plus_minus = score_history.loc[:, ['SCORE_HOME', 'SCORE_AWAY', 'GAME_TIME']].drop_duplicates('GAME_TIME').rename({'SCORE_HOME': 'SCORE', 'SCORE_AWAY': 'VS'}, axis=1).sort_values(by=['GAME_TIME'])
    else:
        plus_minus = score_history.loc[:, ['SCORE_HOME', 'SCORE_AWAY', 'GAME_TIME']].drop_duplicates('GAME_TIME').rename({'SCORE_AWAY': 'SCORE', 'SCORE_HOME': 'VS'}, axis=1).sort_values(by=['GAME_TIME'])

    plus_minus['PLUS_MINUS'] = plus_minus.apply(lambda a: a['SCORE'] - a['VS'], axis=1)

    trace1 = go.Bar(
        x=list(range(0, time_axis.size)),
        y=player_dict[player_name],
        name=player_name,
    )

    trace2 = go.Scatter(
        x=pd.concat([pd.Series([0]), plus_minus['GAME_TIME']]),
        y=pd.concat([pd.Series([0]), plus_minus['PLUS_MINUS']]),
        name='NET SCORE',
        yaxis='y2',
    )

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(trace1,secondary_y=False)
    fig.add_trace(trace2, secondary_y=True)

    # fig = px.bar(
    #     x=list(range(0, time_axis.size)),
    #     y=player_dict[player_name],
    #     title=player_name
    # )

    fig.update_xaxes(
        range=[0, seconds_per_game],
        tickmode='array',
        tickvals=[0, seconds_per_period * 1, seconds_per_period * 2, seconds_per_period * 3],
        ticktext=['1st', '2nd', '3rd', '4th'])

    fig.update_yaxes(
        range=[0, 1],
        tickmode='array',
        tickvals=[],
        ticktext=[],
        secondary_y=False)

    fig.update_yaxes(
        range=[0, 30],
        title_text="<b>secondary</b> yaxis title",
        secondary_y=True)

    return fig


app = Dash(__name__)
app.layout = html.Div([
    html.H1(children='NBA Minutes Distribution Chart', style={'textAlign': 'center'}),
    dcc.Dropdown(id='player_name', options=players, value=players[0]),
    dcc.Graph(id='graph-content')
])

# app.run(debug=True)

if __name__ == '__main__':
    app.run(debug=True)
