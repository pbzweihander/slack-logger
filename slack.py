# Name: slack.py
# Author: pbzweihander
# Email: sd852456@naver.com
#
# Copyright (C) 2017 pbzweihander
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

from slackclient import SlackClient


class Slack:
    client = None
    name = ""
    id = ""
    connected = False
    users = dict()
    channels = dict()

    def __init__(self, token: str, name: str):
        self.client = SlackClient(token)
        self.name = name

        self.refresh_users()
        for user in self.users:
            if self.users[user] == self.name:
                self.id = user
                break
        self.refresh_channels()

    def refresh_users(self):
        for u in self.client.api_call("users.list").get('members'):
            self.users[u.get('id')] = u.get('name')

    def refresh_channels(self):
        for u in self.client.api_call("channels.list").get('channels'):
            self.channels[u.get('id')] = u.get('name')

    def connect(self) -> bool:
        self.connected = self.client.rtm_connect()
        return self.connected

    def post_message(self, chan: str, msg: str, as_user=True, name=""):
        if self.connected:
            self.client.api_call('chat.postMessage', channel=chan, text=msg, as_user=as_user, username=name)

    def read(self) -> list:
        if self.connected:
            return self.client.rtm_read()
