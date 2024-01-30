import sqlalchemy as db
import pandas as pd

def select_rotations_by_team(conn, teamId):
    metadata = db.MetaData()
    rotations_table = db.Table('rotations', metadata)
    query = db.select([rotations_table])
    results = conn.execute(query).fetchall()
    # pd.read_sql(name='rotations', conn=conn)
    pd.read_sql('SELECT * FROM rotations WHERE ""', conn=conn)
    return results

def select_rotations_by_game(conn, gameId):
    # metadata = db.MetaData()
    # rotations_table = db.Table('rotations', metadata)
    # query = db.select([rotations_table])
    # results = conn.execute(query).fetchall()
    # # pd.read_sql(name='rotations', conn=conn)
    results = pd.read_sql(sql='SELECT * FROM rotations WHERE "GAME_ID" = \'{}\''.format(gameId), con=conn)
    return results


def select_scores_by_game(conn, gameId):
    results = pd.read_sql(sql='SELECT * FROM score_histories WHERE "GAME_ID" = \'{}\''.format(gameId), con=conn)
    return results

def select_games_by_id(conn, gameId):
    results = pd.read_sql(sql='SELECT * FROM games WHERE "GAME_ID" = \'{}\''.format(gameId), con=conn)
    return results
