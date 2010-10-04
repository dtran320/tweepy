# Adopted from http://github.com/joshthecoder/tweepy-examples/blob/master/streamwatcher.py
import time
from getpass import getpass
from textwrap import TextWrapper

import tweepy
                           
CONSUMER_KEY = "<YOUR TWITTER CONSUMER KEY HERE>"
CONSUMER_KEY_SECRET = "<YOUR TWITTER CONSUMER KEY SECRET HERE>"
TEST_OAUTH_TOKEN = "<YOUR TWITTER TEST OAUTH TOKEN HERE>"
TEST_OAUTH_TOKEN_SECRET = "<YOUR TWITTER TEST OAUTH TOKEN SECRET HERE>"

class StreamWatcherListener(tweepy.StreamListener):

    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_status(self, status):
        try:
            print self.status_wrapper.fill(status.text)
            print '\n %s  %s  via %s\n' % (status.author.screen_name, status.created_at, status.source)
        except:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        print 'An error has occured! Status code = %s' % status_code
        return True  # keep stream alive

    def on_timeout(self):
        print 'Snoozing Zzzzzz'


def main():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    print "Welcome %s!" % auth.get_username()
    stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)
    
    # Prompt for mode of streaming
    valid_modes = ['sample', 'filter', 'site_stream']
    while True:
        mode = raw_input('Mode? [sample/filter/site_stream] ')
        if mode in valid_modes:
            break
        print 'Invalid mode! Try again.'
    
    if mode == 'sample':
        stream.sample()

    elif mode == 'filter':
        follow_list = raw_input('Users to follow (comma separated): ').strip()
        track_list = raw_input('Keywords to Track (comma separated): ').strip()
        if follow_list: follow_list = [u.strip() for u in follow_list.split(',')]
        else: follow_list = None
        if track_list: track_list = [u.strip() for u in track_list.split(',')]
        else: track_list = None
        stream.filter(follow_list, track_list)
    
    elif mode == 'site_stream':
        stream = tweepy.Stream(auth, tweepy.SiteStreamListener(), timeout=None)
        follow_list = raw_input('Users to follow (comma separated): ').strip()
        if follow_list:
            follow_list = [u for u in follow_list.split(',')]
        else:
            follow_list = None
        stream.site_stream(follow_list)
           
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print '\nGoodbye!'
