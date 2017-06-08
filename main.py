from slack import Slack
from datetime import datetime
import requests
import settings
import time
import json
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

last_search = ()


def es_create(index: str, doc_type: str, body: dict) -> bool:
    query = json.dumps(body)
    res = requests.post('http://localhost:9200/%s/%s/' % (index, doc_type), data=query)
    return json.loads(res.text).get('created') or False


def es_search(index: str, doc_type: str, body: dict, size=10, fr=None) -> list:
    query = {
        'size': size,
        'sort': [{'time': {'order': 'desc'}}],
        'query': {'term': body}}
    if fr:
        query['search_after'] = [fr]
    query = json.dumps(query)
    res = requests.post('http://localhost:9200/%s/%s/_search' % (index, doc_type), data=query)
    jsoned = json.loads(res.text)
    if jsoned.get('hits') and jsoned['hits'].get('hits'):
        return [(float(doc['sort'][0]), doc['_source']) for doc in jsoned['hits']['hits']]
    return []


def handle_message(d: dict):
    channame = slack.channels.get(d['channel'])
    if not channame:
        slack.refresh_channels()
        channame = slack.channels.get(d['channel']) or "?"
    username = slack.users.get(d['user'])
    if not username:
        slack.refresh_users()
        username = slack.users.get(d['user']) or "?"
    date_ts = datetime.fromtimestamp(float(d['ts']))
    log_to_json(channame, username, d['text'], date_ts)
    res = handle_command(d['text'])
    if res:
        slack.post_formatted_message(d['channel'], res)
    return ""


def log_search(key: str, value: str, size=10, fr=None) -> list:
    global last_search
    res = es_search(settings.ES_INDEX, settings.ES_TYPE, {key: value}, size, fr)
    if res:
        outdict = dict()
        outdict['pretext'] = "%s개가 검색됨" % len(res)
        outdict['title'] = "%s: %s" % (key, value)
        outdict['text'] = '\n'.join(["%s %s@%s: %s" %
                                     (doc[1]['time'], doc[1]['user'], doc[1]['channel'], doc[1]['text'])
                                     for doc in res])
        last_search = (key, value, res[-1][0])
        return [outdict]
    else:
        return [{'text': "검색 결과가 없습니다."}]


def log_more(size=10) -> list:
    if not last_search:
        return [{'text': "마지막 검색 결과가 없습니다."}]
    return log_search(last_search[0], last_search[1], size, last_search[2])


def log_help() -> list:
    return [{'text': "Usage:\n"
                     "!logsearch <key> <value> [<size>]\n"
                     "!logmore [<size>]\n"
                     "!loghelp\n"
                     "(key: channel, user, text, time)"}]


def handle_command(text: str) -> list:
    if text.startswith("!"):
        if text.split()[0] == "!logsearch":
            args = text.split()[1:]
            if len(args) < 2 or len(args) > 3:
                return log_help()
            if len(args) == 3:
                return log_search(args[0], args[1], int(args[2]))
            else:
                return log_search(args[0], args[1])
        elif text.split()[0] == "!logmore":
            args = text.split()
            if len(args) > 1:
                return log_more(int(args[1]))
            else:
                return log_more()
        elif text.split()[0] == "!loghelp":
            return log_help()
    return []


def log_to_json(channel: str, user: str, text: str, date_ts: datetime):
    outdict = dict()
    outdict['channel'] = channel
    outdict['user'] = user
    outdict['text'] = text
    outdict['time'] = date_ts.strftime('%Y/%m/%d %H:%M:%S')
    logger.info(json.dumps(outdict, ensure_ascii=False))
    es_create(settings.ES_INDEX, settings.ES_TYPE, outdict)


def main():
    print("Logger Started!")
    while True:
        raw_text = slack.read()
        if not raw_text:
            continue
        text = json.loads(raw_text)
        if text.get('type') == 'message' and 'subtype' not in text and text.get('text'):
            handle_message(json.loads(raw_text))
        time.sleep(0.01)


if __name__ == '__main__':
    main()
