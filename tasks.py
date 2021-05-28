# tasks.py
import json  # exporting to files
import re
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
        # execute my request, parse the data using the XML
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
        soup = BeautifulSoup(r.content, features='html.parser')
        # select only the "items" I want from the data
        items = soup.find_all('item')
        item_dict = {n: list(row) for n in range(len(items)) for row in items}
        rows = [row[3:13] for row in item_dict.values()]
        # stripped_rows = list(map(remove_html_tags, rows))
        for row in rows:
            print(row[0])
            print()
        # print(rows)
        # print(items)
        tables = []
        # items is iterable
        # print(tables)

        # for each "item" I want, parse it into a list
        for halt in tables:
            symbol = halt.find('ndaq:IssueSymbol').text
            company_name = halt.find('ndaq:IssueName').text
            halt_code = halt.find('ndaq:ReasonCode').text
            halt_date = halt.find('ndaq:HaltDate').text
            halt_time = halt.find('ndaq:HaltTime').text
            resumption_date = halt.find('ndaq:ResumptionDate').text
            resumption_time = halt.find('ndaq:ResumptionTime').text
            # create an "halt_record" object with the data
            # from each "item"
            halt_record = {
                'symbol': symbol,
                'company_name': company_name,
                'halt_code': halt_code,
                'halt_date': halt_date,
                'halt_time': halt_time,
                'resumption_date': resumption_date,
                'resumption_time': resumption_time,
                'created_at': str(datetime.now()),
                'source': 'NasdaqTrader RSS'
            }
            print(halt_record)
            # append my "trade_halt_list" with each "halt_record" object
            trade_halt_list.append(halt_record)
        print('Finished scraping the trade halts')
        print(trade_halt_list)
        # after the loop, dump my saved objects into a .json file
        return save_function(trade_halt_list)
    except Exception as e:
        print('The scraping job failed. See exception: ')
        print(e)



