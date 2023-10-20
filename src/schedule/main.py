from apscheduler.schedulers.blocking import BlockingScheduler
from src.jobs_.create_subs_up_to_date.main import create_subs_up_to_date as create
import time

sched = BlockingScheduler()

season_id = '2023-24'
season_type= ['Pre Season', 'Regular Season', 'Playoffs']

# @sched.scheduled_job('cron', day_of_week='mon-sun', hour=5)
# def scheduled_job():
#     result = False
#     while not result:
#         result = create(date="10/16/2023", season_id=season_id, season_type=season_type[0])
#         time.sleep(5)

@sched.scheduled_job('interval', minutes=3)
def timed_job():
    print('This job is run every three minutes.')

sched.start()