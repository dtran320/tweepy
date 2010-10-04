# Tweepy
# Copyright 2009-2010 Joshua Roesslein
# See LICENSE for details.

import httplib
from socket import timeout
from threading import Thread
from time import sleep
import urllib

from tweepy.auth import BasicAuthHandler
from tweepy.models import Status
from tweepy.api import API
from tweepy.error import TweepError

from tweepy.utils import import_simplejson
json = import_simplejson()

STREAM_VERSION = 1

STREAM_HOST = 'stream.twitter.com'
SITE_STREAM_HOST = 'betastream.twitter.com'
APP_NAME = 'conversely1.0'

class StreamListener(object):

    def __init__(self, api=None):
        self.api = api or API()

    def on_data(self, data):
        """Called when raw data is received from connection.

        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """
        print data
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
                        if self.on_follow(user_id, source=message[u'source'], target=message[u'target'], time=message[u'created_at']) is False:
                            return False
                elif u'retweeted_status' in message:
                    if self.on_retweet(user_id, message) is False:
                        return False
                elif u'text' in message:
                    status = Status.parse(self.api, message)
                    if self.on_status(user_id, status) is False:
                        return False
                elif u'direct_message' in message:
                    if self.on_direct_message(user_id, message[u'direct_message']) is False:
                        return False
                else:
                    print parsed_data
                
    def on_friends(self, user_id, friends_list):
        print "Friends for %d: %s" % (user_id, ",".join([str(friend) for friend in friends_list]))
            
    def on_status(self, user_id, status):
        print "%d Status: %s" % (user_id, status)
    
    def on_follow(self, user_id, source, target, time):
        """follow has a source, target and created_at"""
        print "%s Followed by %s at %s" % (target[u'name'], source[u'name'], time)
    
    def on_retweet(self, user_id, retweet):
        print retweet
        print "%d Retweeted by %s" % (user_id, retweet[u'user'][u'name'])
    
    def on_direct_message(self, user_id, message):
        print "%s Received DM: %s from %s" % (message[u'recipient'][u'name'], message[u'text'], message[u'sender'][u'name'])
    
    def on_error(self, status_code):
        print 'An error has occured! Status code = %s' % status_code
        return True  # keep stream alive

    def on_timeout(self):
        print 'Snoozing Zzzzzz'
                
        
class Stream(object):

    def __init__(self, auth_handler, listener, timeout=5.0, retry_count = None,
                    retry_time = 10.0, snooze_time = 5.0, buffer_size=1500, headers=None):
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
        self.scheme = "http://"
        self.parameters = {}
        self.headers['User-Agent'] = APP_NAME
    def _run(self):
        # setup
        #self.url = "%s?%s" % (self.url, urllib.urlencode(self.parameters))
        auth_url = "%s%s%s" % (self.scheme, self.host, self.url)
        self.auth.apply_auth(url=auth_url, method="POST", headers=self.headers, parameters=self.parameters)
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
                conn.set_debuglevel(1)
                conn.connect()
                conn.sock.settimeout(self.timeout)
                conn.request(method='POST', url=self.url, body=self.body, headers=self.headers)
                resp = conn.getresponse()
                if resp.status != 200:
                    if self.listener.on_error(resp.status) is False:
                        break
                    error_counter += 1
                    sleep(self.retry_time)
                else:
                    error_counter = 0
                    self._read_loop(resp)
            except timeout:
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
        self.url = '/%i/statuses/firehose.json?delimited=length' % STREAM_VERSION
        if count:
            self.url += '&count=%s' % count
        self._start(async)

    def retweet(self, async=False):
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/retweet.json?delimited=length' % STREAM_VERSION
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

