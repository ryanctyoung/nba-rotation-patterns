import pickle
from datetime import date, timedelta


def store_date():
    # initializing data to be stored in db
    today = date.today()

    # database
    db = {'last_modified': today}

    # Its important to use binary mode
    dbfile = open('../../pickle/date_last_completed', 'wb')

    # source, destination
    pickle.dump(db, dbfile)
    dbfile.close()


def load_date():
    # for reading also binary mode is important
    dbfile = open('../../pickle/date_last_completed', 'rb')
    db = pickle.load(dbfile)
    last_modified = db['last_modified']
    dbfile.close()
    return last_modified

if __name__ == '__main__':
    today = date.today() - timedelta(days=4)
    db = {'last_modified': today}

    # Its important to use binary mode
    dbfile = open('../../pickle/date_last_completed', 'wb')

    # source, destination
    pickle.dump(db, dbfile)
    dbfile.close()

