# tasks.py
import json  # exporting to files
from datetime import datetime, timedelta, date

import requests  # pulling data
from bs4 import BeautifulSoup  # xml parsing
from celery import Celery
from celery.schedules import crontab  # scheduler

app = Celery('tasks')

app.conf.timezone = 'UTC'

# scheduled task execution
app.conf.beat_schedule = {
    # executes every 1 minute
    'scraping-task-one-min': {
        'task': 'tasks.nasdaqtrader_halts_rss',
        'schedule': crontab(),
    }
}


@app.task
def save_function(article_list):
    # timestamp and filename
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    filename = 'articles-{}.json'.format(timestamp)

    with open(filename, 'w') as outfile:
        json.dump(article_list, outfile)


@app.task
def nasdaqtrader_halts_rss():
    trade_halt_list = []
    try:
        # execute my request, parse the data using the LXML
        # parser in BS4

        ##### TODO: Create a URL shortener
        today_date = date.today()
        today_date_str = today_date.strftime('%m%d%Y')
        yesterday_date = (today_date - timedelta(days=1))
        yesterday_date_str = yesterday_date.strftime('%m%d%Y')
        query_string = 'http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts&haltdate={}&resumedate={}'.format(
            yesterday_date_str, today_date_str)
        print(query_string)
        r = requests.get(query_string)
        soup = BeautifulSoup(r.content, 'lxml')
        # select only the "items" I want from the data
        trade_halts = soup.find_all('item')
        # create a dictionary from soup Result Set for each halt and add to the current query's list
        for trade_halt in trade_halts:
            trade_halt_record = {child.name: child.text for child in trade_halt.findChildren()}
            trade_halt_list.append(trade_halt_record)

        print('Finished scraping the trade halts')
        # after the loop, dump my saved objects into a .json file
        return save_function(trade_halt_list)
    except Exception as e:
        print('The scraping job failed. See exception: ')
        print(e)
