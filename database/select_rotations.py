import sqlalchemy as db


def select_rotations(conn):
    metadata = db.MetaData()
    rotations_table = db.Table('rotations', metadata)
    query = db.select([rotations_table])
    results = conn.execute(query).fetchall()
    return results
