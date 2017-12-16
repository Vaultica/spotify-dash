#!/usr/bin/env python

import spotipy
import spotipy.util as util
from config import CLIENT_ID, CLIENT_SECRET, PLAY_LIST, USER
import random
import pandas as pd

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out
'''
# Fetch all  albums

idx = 0
all_albums = pd.DataFrame()
scope = 'user-library-read'

while True:
    try:
        results = sp.current_user_saved_albums(limit=10, offset=(10*idx)+1)
    except:
        token = util.prompt_for_user_token(USER, scope)
        sp = spotipy.Spotify(auth=token)
        results = sp.current_user_saved_albums(limit=10, offset=(10*idx)+1)
        print ("obtained new token {}".format(token))

    if len(results['items']) == 0:
        break
    idx += 1
    print ('Fetching page {}'.format(idx))
    dic_flattened = [flatten_json(d) for d in results['items']]
    all_albums = all_albums.append(pd.DataFrame(dic_flattened))

print ('All albums fetched')

all_albums[['album_name','album_artists_0_name', 'album_uri']].sort_values(by='album_artists_0_name')\
     .to_excel('all_albums.xlsx')
'''
# Get recently played tracks
import requests

scope = 'user-read-recently-played'

url = "https://api.spotify.com/v1/me/player/recently-played?type=track&limit=50"
headers = {
    'authorization': "Bearer BQAJDrOFPiOIsbEDpQsBHn1LTzGTvPkhCXtgOPW-xnO-RC0tPUhDT-McYWqNTBqFpfbZJAmOP1vX6DRcZPXDWJIrb2dEHV7jfExADf6_N0amKQbhqiUo5IPI28t1S2l2wTMQLaAz5ejHuSAyu1HWHptr_vbOM3RuCv3LbMpk",
    'cache-control': "no-cache"
    }

response = requests.request("GET", url, headers=headers)

if response.status_code >= 401:
    token = util.prompt_for_user_token(USER, scope)
    response = requests.request("GET", url, headers={"Authorization": "Bearer %s" %token})
    print ("request with new bearer code {})".format(token))

from pandas.io.json import json_normalize #package for flattening json in pandas df

tracks = response.json()

dic_flattened = [flatten_json(d) for d in tracks['items']]
df = pd.DataFrame(dic_flattened)

df[['track_name','played_at', 'track_album_artists_0_name', 'track_album_name', 'track_type', 'track_uri']] \
  .to_excel('recently_played.xlsx')

# Save tracks in Google BigQuery
from google.cloud import bigquery
from pandas.io import gbq

project_id='secret-compass-181513'
bigquery_client = bigquery.Client(project='secret-compass-181513')
dataset_name = 'spotify'
dataset = bigquery_client.dataset(dataset_name)

df_recently_played = df[['track_name','played_at', 'track_album_artists_0_name', 'track_album_name', 'track_type', 'track_uri']]

gbq.to_gbq(df_recently_played, 'spotify.play_history', project_id, if_exists='append')

# Retrieve and show results
#df = gbq.read_gbq('SELECT * FROM [secret-compass-181513:spotify.playback_history] order by played_at desc LIMIT 1000', project_id)
#df ['track_url'] = df['track_uri'].str.replace(r'spotify:track:', 'https://open.spotify.com/track/')
#df.head(20)
