from apscheduler.schedulers.blocking import BlockingScheduler
from src.jobs_.create_subs_up_to_date.main import create_subs_up_to_date as create
import time
from generic.save_last_modified.main import load_date, store_date

sched = BlockingScheduler()

season_id = '2023-24'
season_type = ['Pre Season', 'Regular Season', 'Playoffs']

@sched.scheduled_job('cron', day_of_week='mon-sun', hour=5)
def scheduled_job():
    result = False
    last_modified = load_date()
    try:
        while not result:
            result = create(date=last_modified, season_id=season_id, season_type=season_type[0])
            time.sleep(5)
    except Exception as error:
        return
    store_date()

# @sched.scheduled_job('interval', minutes=3)
# def timed_job():
#     print('This job is run every three minutes.')

sched.start()