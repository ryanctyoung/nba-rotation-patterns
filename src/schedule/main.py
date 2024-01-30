from src.jobs_.create_subs_up_to_date.main import create_subs_up_to_date as create
from generic.print_to_log.main import print_to_log
import time
from generic.save_last_modified.main import load_date, store_date
from datetime import date

season_id = '2023-24'
season_type = ['Pre Season', 'Regular Season', 'Playoffs']


def scheduled_job():
    print_to_log('Beginning daily scheduled job.')
    result = False
    last_modified = load_date()
    print_to_log('Last ran on {}'.format(last_modified))

    if last_modified == date.today():
        print_to_log('No new games to input. Ending job.'.format(last_modified))
        return

    try:
        while not result:
            result = create(date=last_modified, season_id=season_id, season_type=season_type[1])
            if not result:
                time.sleep(5)
    except Exception as error:
        print_to_log(error)
        return
    store_date()


if __name__ == '__main__':
    scheduled_job()
