# Name: slack_wrapper.py
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

import slacker
import websocket
import time


class Slack:
    client = None
    socket = None
    name = ""
    id = ""
    users = dict()
    channels = dict()

    def __init__(self, token: str, name: str):
        self.client = slacker.Slacker(token)
        self.connect_socket()
        self.name = name

        self.refresh_users()
        for user in self.users:
            if self.users[user] == self.name:
                self.id = user
                break
        self.refresh_channels()

    def connect_socket(self):
        try:
            res = self.client.rtm.start()
            rtm_endpoint = res.body['url']
            self.socket = websocket.create_connection(rtm_endpoint)
        except:
            time.sleep(1)
            self.connect_socket()

    def refresh_users(self):
        for u in self.client.users.list().body['members']:
            self.users[u.get('id')] = u.get('name')

    def refresh_channels(self):
        for u in self.client.channels.list().body['channels']:
            self.channels[u.get('id')] = u.get('name')

    def post_message(self, chan: str, msg: str, as_user=True, name=""):
        self.client.chat.post_message(channel=chan, text=msg, as_user=as_user, username=name)

    def post_formatted_message(self, chan: str, body: list, as_user=True, name=""):
        self.client.chat.post_message(channel=chan, text=None, attachments=body, as_user=as_user, username=name)

    def read(self) -> str:
        try:
            text = self.socket.recv()
        except websocket.WebSocketConnectionClosedException:
            print('connection error, reconnecting...')
            time.sleep(1)
            self.connect_socket()
            return self.read()
        return text
