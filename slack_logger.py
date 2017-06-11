import datetime
import elasticsearch_wrapper
import slack_wrapper
import settings
import json
import logging.handlers


class SlackLogger:
    slack = slack_wrapper.Slack(settings.SLACK_TOKEN, settings.BOT_NAME)
    logger = logging.getLogger('slack-logger')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    file_handler = logging.handlers.TimedRotatingFileHandler(
        './logs/slack-log', when='midnight', interval=1, encoding="UTF-8")
    file_handler.suffix = "%Y-%m-%d"
    stream_handler = logging.StreamHandler()

    last_search = ()

    def __init__(self):
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.stream_handler)

    def handle_message(self, d: dict):
        channame = self.slack.channels.get(d['channel'])
        if not channame:
            self.slack.refresh_channels()
            channame = self.slack.channels.get(d['channel']) or "?"
        username = self.slack.users.get(d['user'])
        if not username:
            self.slack.refresh_users()
            username = self.slack.users.get(d['user']) or "?"
        date_ts = datetime.datetime.fromtimestamp(float(d['ts']))
        self.log_to_json(channame, username, d['text'], date_ts)
        res = self.handle_command(d['text'])
        if res:
            self.slack.post_formatted_message(d['channel'], res)
        return ""

    def log_search(self, filters: list, size=10, fr=None) -> list:
        if len(filters) > 1:
            res = elasticsearch_wrapper.es_filter_search(settings.ES_INDEX, settings.ES_TYPE, filters, size, fr)
        else:
            res = elasticsearch_wrapper.es_single_search(settings.ES_INDEX, settings.ES_TYPE, {filters[0][0]: filters[0][1]}, size, fr)
        outdict = dict()
        outdict['pretext'] = "%s개가 검색됨" % len(res)
        outdict['title'] = ', '.join(["%s: %s" % (f[0], f[1]) for f in filters])
        if res:
            outdict['text'] = '\n'.join(["%s %s@%s: %s" %
                                         (doc[1]['time'], doc[1]['user'], doc[1]['channel'], doc[1]['text'])
                                         for doc in res][::-1])
            self.last_search = (filters, res[-1][0])
        else:
            outdict['text'] = "검색 결과가 없습니다."
        return [outdict]

    def log_more(self, size=10) -> list:
        if not self.last_search:
            return [{'text': "마지막 검색 결과가 없습니다."}]
        return self.log_search(self.last_search[0], size, self.last_search[1])

    @staticmethod
    def log_help() -> list:
        return [{'text': "Usage:\n"
                         "!logsearch <key1>:<value1> [<key2>:<value2> ...]\n"
                         "!logmore [<size>]\n"
                         "!loghelp\n"
                         "(key: channel, user, text, time)"}]

    def handle_command(self, text: str) -> list:
        if text.startswith("!"):
            if text.split()[0] == "!logsearch":
                filters = [tuple(map(str.strip, f.split(':')))
                           for f in text.split() if ':' in f]
                if not filters:
                    return self.log_help()
                return self.log_search(filters)
            elif text.split()[0] == "!logmore":
                args = text.split()
                if len(args) > 1:
                    return self.log_more(int(args[1]))
                else:
                    return self.log_more()
            elif text.split()[0] == "!loghelp":
                return self.log_help()
        return []

    def log_to_json(self, channel: str, user: str, text: str, date_ts: datetime):
        outdict = dict()
        outdict['channel'] = channel
        outdict['user'] = user
        outdict['text'] = text
        outdict['time'] = date_ts.strftime('%Y/%m/%d %H:%M:%S')
        self.logger.info(json.dumps(outdict, ensure_ascii=False))
        elasticsearch_wrapper.es_create(settings.ES_INDEX, settings.ES_TYPE, outdict)

    def run(self):
        while True:
            raw_text = self.slack.read()
            if not raw_text:
                continue
            text = json.loads(raw_text)
            if text.get('type') == 'message' and 'subtype' not in text and text.get('text'):
                self.handle_message(json.loads(raw_text))
