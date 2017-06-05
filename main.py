
from slack import Slack
from datetime import datetime
import settings
import time
import json
import logging
import logging.handlers


slack = Slack(settings.SLACK_TOKEN, settings.BOT_NAME)
slack.connect()
logger = logging.getLogger('slack-logger')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
file_handler = logging.handlers.TimedRotatingFileHandler(
    './slack-log', when='midnight', interval=1, encoding="UTF-8")
file_handler.suffix = "%Y-%m-%d"
stream_handler = logging.StreamHandler()
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def log_to_json(channel: str, user: str, text: str, ts: float):
    outdict = dict()
    channame = slack.channels.get(channel)
    if not channame:
        slack.refresh_channels()
        channame = slack.channels.get(channel) or "?"
    username = slack.users.get(user)
    if not username:
        slack.refresh_users()
        username = slack.users.get(user) or "?"
    outdict['channel'] = channame
    outdict['user'] = username
    outdict['text'] = text
    outdict['time'] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(json.dumps(outdict, ensure_ascii=False))
    return ""


def main():
    print("Logger Started!")
    while True:
        texts = slack.read()
        if not texts:
            continue
        _ = [log_to_json(text.get('channel'), text.get('user'), text.get('text'), float(text.get('ts')))
             for text in texts
             if text.get('type') == 'message'
             if 'subtype' not in text
             if text.get('text')]
        time.sleep(0.01)


if __name__ == '__main__':
    main()
