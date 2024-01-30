import sqlalchemy as db
import pandas as pd
from database.connect import get_sql_session
from database.select_queries import select_rotations_by_game, select_scores_by_game, select_games_by_id


if __name__ == '__main__':
    game_id = "0022300061"


    session = get_sql_session()
    with session.connect() as conn:
        game_boxes = select_games_by_id(conn, game_id)


    print(game_boxes)
