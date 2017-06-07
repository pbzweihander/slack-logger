# slack-logger

Slack 내용을 json으로 로깅해주는 봇

### Setting

`settings.default.py` 파일을 `settings.py`로 복사 후 수정

- BOT_NAME : 봇의 닉네임
- SLACK_TOKEN : Slack Bot 토큰

Slack에서 로깅을 원하는 채널에 봇을 초대하면 로깅이 시작된다.

### Requirements

- requests
- slacker
- websocket-client

--------

[GNU AGPL 3.0 License](LICENSE.md)

[pbzweihander](https://github.com/pbzweihander)
