from slack import Slack
from datetime import datetime
from elasticsearch import Elasticsearch
import settings
import time
import json
import logging
import logging.handlers

slack = Slack(settings.SLACK_TOKEN, settings.BOT_NAME)
logger = logging.getLogger('slack-logger')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
file_handler = logging.handlers.TimedRotatingFileHandler(
    './logs/slack-log', when='midnight', interval=1, encoding="UTF-8")
file_handler.suffix = "%Y-%m-%d"
stream_handler = logging.StreamHandler()
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
es_client = Elasticsearch()


def es_create(index: str, doc_type: str, body: dict) -> bool:
    res = es_client.create(index, doc_type, body)
    return res.get('created') or False


def es_search(index: str, doc_type: str, body: dict) -> list:
    query = { 'query': { 'term': {} } }
    query['query']['term'].update(body)
    res = es_client.search(index, doc_type, query)
    if res.get('hits') and res['hits'].get('hits'):
        return [doc.get('_source') for doc in res['hits']['hits']]
    return []


def handle_message(channel:str, user: str, text: str, ts: float):
    channame = slack.channels.get(channel)
    if not channame:
        slack.refresh_channels()
        channame = slack.channels.get(channel) or "?"
    username = slack.users.get(user)
    if not username:
        slack.refresh_users()
        username = slack.users.get(user) or "?"
    date_ts = datetime.fromtimestamp(ts)
    log_to_json(channame, username, text, date_ts)
    return ""


def log_to_json(channel: str, user: str, text: str, date_ts: datetime):
    outdict = dict()
    outdict['channel'] = channame
    outdict['user'] = username
    outdict['text'] = text
    outdict['ts'] = dict()
    outdict['ts']['date'] = date_ts.strftime('%Y-%m-%d')
    outdict['ts']['time'] = date_ts.strftime('%H:%M:%S')
    logger.info(json.dumps(outdict, ensure_ascii=False))
    es_create(settings.ES_INDEX, setting.ES_TYPE, outdict)


def main():
    print("Logger Started!")
    while True:
        texts = slack.read()
        if not texts:
            continue
        _ = [handle_message(text.get('channel'), text.get('user'), text.get('text'), float(text.get('ts')))
             for text in texts
             if text.get('type') == 'message'
             if 'subtype' not in text
             if text.get('text')]
        time.sleep(0.01)


if __name__ == '__main__':
    main()

