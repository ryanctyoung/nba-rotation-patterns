from src.jobs_.create_subs_up_to_date.main import create_subs_up_to_date as create
import time
from generic.save_last_modified.main import load_date, store_date
from datetime import date, timedelta


season_id = '2023-24'
season_type = ['Pre Season', 'Regular Season', 'Playoffs']

def scheduled_job():
    print('Beginning daily scheduled job.')
    result = False
    last_modified = load_date()
    print('Last ran on {}'.format(last_modified))

    if last_modified == date.today():
        return

    try:
        while not result:
            result = create(date=last_modified, season_id=season_id, season_type=season_type[1])
            if not result:
                time.sleep(5)
    except Exception as error:
        print(error)
        return
    store_date()

if __name__ == '__main__':
    scheduled_job()


