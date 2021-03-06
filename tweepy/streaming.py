# Tweepy
# Copyright 2009-2010 Joshua Roesslein
# See LICENSE for details.

import httplib
import socket
from socket import timeout
from threading import Thread
from time import sleep
import urllib

from tweepy.models import Status
from tweepy.api import API
from tweepy.error import TweepError

from tweepy.utils import import_simplejson
json = import_simplejson()

import settings

from django.utils.encoding import smart_unicode

STREAM_VERSION = 1

STREAM_HOST = 'stream.twitter.com'
SITE_STREAM_HOST = 'sitestream.twitter.com'
APP_NAME = settings.APP_NAME


class StreamListener(object):

    def __init__(self, api=None):
        self.api = api or API()

    def on_data(self, data):
        """Called when raw data is received from connection.

        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """
        if 'in_reply_to_status_id' in data:
            status = Status.parse(self.api, json.loads(data))
            if self.on_status(status) is False:
                return False
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False

    def on_status(self, status):
        """Called when a new status arrives"""
        return True

    def on_delete(self, status_id, user_id):
        """Called when a delete notice arrives for a status"""
        return True

    def on_limit(self, track):
        """Called when a limitation notice arrvies"""
        return True

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        return False

    def on_timeout(self):
        """Called when stream connection times out"""
        return True


class SiteStreamListener(object):
    def __init__(self, api=None):
        self.api = api or API()

    def on_data(self, data):
        """
        Generic class for site streams that just print each
        action that comes in - override these methods to actually
        process them
        """
        if 'for_user' in data:
            parsed_data = json.loads(data)
            user_id = parsed_data['for_user']
            if 'message' in data:
                message = parsed_data['message']
                if u'friends' in message:
                    if self.on_friends(user_id, message['friends']) is False:
                        return False
                elif u'event' in message:
                    if message[u'event'] == u'follow':
                        if self.on_follow(
                            user_id=user_id,
                            source=message[u'source'],
                            target=message[u'target'],
                            time=message[u'created_at']
                        ) is False:
                            return False
                    elif message[u'event'] == u'unfollow':
                        if self.on_unfollow(
                            user_id,
                            source=message[u'source'],
                            target=message[u'target'],
                            time=message[u'created_at']
                        ) is False:
                            return False
                    elif message[u'event'] == u'favorite':
                        if self.on_favorite(
                            user_id,
                            source=message[u'source'],
                            favorited=message[u'target_object'],
                            time=message[u'created_at']
                        ) is False:
                            return False
                    elif message[u'event'] == u'unfavorite':
                        if self.on_unfavorite(
                            user_id,
                            source=message[u'source'],
                            favorited=message[u'target_object']
                        ) is False:
                            return False
                # Need this second check - could be a retweet of
                # a tweet mentioning the user of interest
                elif (u'retweeted_status' in message and
                    int(message[u'retweeted_status'][u'user'][u'id']) ==
                    int(user_id)
                ):
                    if self.on_retweet(user_id, message) is False:
                        return False
                elif u'text' in message:
                    status = Status.parse(self.api, message)
                    # tweet from the user of interest
                    if status.author.id == user_id:
                        if self.on_user_status(user_id, status) is False:
                            return False
                    else:   # tweet mentioning the user of interest
                        if self.on_user_mention(user_id, status) is False:
                            return False
                elif u'direct_message' in message:
                    if self.on_direct_message(
                        user_id, message[u'direct_message']
                    ) is False:
                        return False
                else:
                    print parsed_data

    def on_friends(self, user_id, friends_list):
        print "Friends for %d: %s" % (
            user_id,
            ",".join([str(friend) for friend in friends_list])
        )

    def on_user_status(self, user_id, status):
        print "%s: %s" % (status.author.screen_name, status.text)

    def on_user_mention(self, user_id, status):
        print "%s: %s" % (status.author.screen_name, status.text)

    def on_follow(self, user_id, source, target, time):
        """follow has a source, target and created_at"""
        print "%s Followed by %s at %s" % (
            target[u'name'],
            source[u'name'],
            time
        )

    def on_unfollow(self, user_id, source, target, time):
        """unfollow has a source, target and created_at"""
        print "%s Unfollowed by %s at %s" % (
            target[u'name'],
            source[u'name'],
            time
        )

    def on_retweet(self, user_id, retweet):
        print "%s Retweeted by %s" % (
            retweet[u'retweeted_status'][u'user'][u'name'],
            retweet[u'user'][u'name']
        )

    def on_direct_message(self, user_id, message):
        print "%s Received DM: %s from %s" % (
            message[u'recipient'][u'name'],
            message[u'text'],
            message[u'sender'][u'name']
        )

    def on_favorite(self, user_id, source, favorited, time):
        print "%s favorited %s's tweet: %s at %s" % (
            source[u'name'],
            favorited[u'user'][u'name'],
            favorited[u'text'],
            time
        )

    def on_unfavorite(self, user_id, source, favorited):
        print "%s unfavorited %s's tweet: %s" % (
            source[u'name'],
            favorited[u'user'][u'name'],
            favorited[u'text']
        )

    def on_capacity_error(self, stream_id):
        return True

    def on_error(self, status_code, stream_id, error_counter, threshold):
        if status_code == 401 and error_counter >= threshold:
            return self.on_capacity_error(stream_id)
        else:
            print 'An error has occured in stream %d! Status code = %s' % (
                stream_id, status_code
            )
            return True  # keep stream alive

    def on_timeout(self):
        print 'Snoozing Zzzzzz'


class Stream(object):

    def __init__(self, auth_handler, listener, timeout=10.0, retry_count=None,
                    retry_time=10.0, snooze_time=5.0, buffer_size=1500,
                    headers=None, debug=False, stream_id=None,
                    capacity_error_threshold=3):
        self.auth = auth_handler
        self.running = False
        self.timeout = timeout
        self.retry_count = retry_count
        self.retry_time = retry_time
        self.snooze_time = snooze_time
        self.buffer_size = buffer_size
        self.listener = listener
        self.api = API()
        self.headers = headers or {}
        self.body = None
        self.host = STREAM_HOST
        self.scheme = "https://"
        self.parameters = {}
        self.headers['User-Agent'] = APP_NAME
        self.debug = debug
        self.stream_id = stream_id  # Used by external apps to pass an id
        self.capacity_error_threshold = capacity_error_threshold

    def _run(self):
        # setup
        #self.url = "%s?%s" % (self.url, urllib.urlencode(self.parameters))
        auth_url = "%s%s%s" % (self.scheme, self.host, self.url)
        self.auth.apply_auth(
            url=auth_url,
            method="POST",
            headers=self.headers,
            parameters=self.parameters
        )

        # enter loop
        error_counter = 0
        conn = None
        exception = None
        while self.running:
            if self.retry_count and error_counter > self.retry_count:
                # quit if error count greater than retry count
                break
            try:
                if self.scheme == "https://":
                    conn = httplib.HTTPSConnection(self.host)
                else:
                    conn = httplib.HTTPConnection(self.host)

                if self.debug:
                    conn.set_debuglevel(1)

                conn.connect()
                conn.sock.settimeout(self.timeout)
                conn.request(
                    method='POST',
                    url=self.url,
                    body=self.body,
                    headers=self.headers
                )
                resp = conn.getresponse()
                if resp.status != 200:
                    if self.listener.on_error(
                        resp.status, self.stream_id, error_counter,
                        self.capacity_error_threshold
                    ) is False:
                        break
                    error_counter += 1
                    # sleep longer for twitter capacity issues
                    if resp.status == 401:
                        sleep(2 * self.retry_time)
                    else:
                        sleep(self.retry_time)
                else:
                    error_counter = 0
                    self._read_loop(resp)
            except (timeout, httplib.IncompleteRead, socket.error):
                if self.listener.on_timeout() == False:
                    break
                if self.running is False:
                    break
                conn.close()
                sleep(self.snooze_time)
            except Exception, exception:
                # any other exception is fatal, so kill loop
                break

        # cleanup
        self.running = False
        if conn:
            conn.close()

        if exception:
            raise exception

    def _read_loop(self, resp):
        data = ''
        while self.running:
            if resp.isclosed():
                break
            # read length
            length = ''
            while True:
                c = resp.read(1)
                if c == '\n':
                    break
                length += c
            length = length.strip()
            if length.isdigit():
                length = int(length)
            else:
                continue

            # read data and pass into listener
            data = resp.read(length)
            # Address UnicodeEncodeErrors
            data = smart_unicode(data)
            if self.listener.on_data(data) is False:
                self.running = False

    def _start(self, async):
        self.running = True
        if async:
            Thread(target=self._run).start()
        else:
            self._run()

    def firehose(self, count=None, async=False):
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/firehose.json?delimited=length' % (
            STREAM_VERSION
        )
        if count:
            self.url += '&count=%s' % count
        self._start(async)

    def retweet(self, async=False):
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/retweet.json?delimited=length' % (
            STREAM_VERSION
        )
        self._start(async)

    def sample(self, count=None, async=False):
        params = {'delimited': 'length'}
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/sample.json' % STREAM_VERSION
        if count:
            params['count'] = count
        self.parameters = params
        self.body = urllib.urlencode(params)
        self._start(async)

    def filter(self, follow=None, track=None, async=False, locations=None):
        params = {'delimited': 'length'}
        self.headers['Content-type'] = "application/x-www-form-urlencoded"
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/filter.json' % STREAM_VERSION
        if follow:
            params['follow'] = ','.join(map(str, follow))
        if track:
            params['track'] = ','.join(map(str, track))
        if locations and len(locations) > 0:
            assert len(locations) % 4 == 0
            params['locations'] = ','.join(['%.2f' % l for l in locations])
        self.parameters = params
        self.body = urllib.urlencode(params)
        self._start(async)

    def site_stream(self, follow=None, async=False):
        params = {'delimited': 'length'}
        self.headers['Content-type'] = "application/x-www-form-urlencoded"
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/2b/site.json'
        if follow:
            params['follow'] = ','.join(map(str, follow))
        self.parameters = params
        self.body = urllib.urlencode(params)
        self.host = SITE_STREAM_HOST
#        self.scheme = "https://"
        self._start(async)

    def disconnect(self):
        if self.running is False:
            return
        self.running = False
