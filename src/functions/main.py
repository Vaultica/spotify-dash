# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import time
import json
import pandas as pd

from google.cloud import storage
from google.cloud import bigquery
from flask import escape

import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyOAuth

USER = 'SERGEBOU'
bucket_name = "secret-compass-181513"


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

def load_tracks_bq(tracks):
    """Loads json to BigQuery table."""
    from pandas.io import gbq
    from pandas.io.json import json_normalize #package for flattening json in pandas df

    # Constants
    project_id   = 'secret-compass-181513'
    dataset_name = 'spotify'

    dic_flattened = [flatten_json(d) for d in tracks['items']]
    df = pd.DataFrame(dic_flattened)

    # Save tracks in Google BigQuery
    bigquery_client = bigquery.Client(project=project_id)
    dataset = bigquery_client.dataset(dataset_name)

    df_recently_played = df[['track_name',
                             'played_at',
                             'track_album_artists_0_name',
                             'track_album_name',
                             'track_type',
                             'track_uri']]

    gbq.to_gbq(df_recently_played, 'spotify.play_history_new', project_id, if_exists='append')

    return 'dataset loaded to BQ'

def load_albums_bq(albums):
    """Loads json to BigQuery table."""
    from pandas.io import gbq

    # Constants
    project_id   = 'secret-compass-181513'
    dataset_name = 'spotify'

    dic_flattened = [flatten_json(d) for d in albums['items']]
    df = pd.DataFrame(dic_flattened)

    # Save albums in Google BigQuery
    bigquery_client = bigquery.Client(project=project_id)
    dataset = bigquery_client.dataset(dataset_name)

    df_recent_albums = df[['album_uri',
                           'added_at',
                           'album_artists_0_name',
                           'album_name',
                           'album_type' ,
                           'album_label',
                           'album_release_date',
                           'album_tracks_total',
                           'album_id']]

    gbq.to_gbq(df_recent_albums, 'spotify.new_albums', project_id, if_exists='append')

    return 'dataset loaded to BQ'

def load_all_albums_bq(albums):
    """Loads json to BigQuery table."""
    from pandas.io import gbq

    # Constants
    project_id   = 'secret-compass-181513'
    dataset_name = 'spotify'

    dic_flattened = [flatten_json(d) for d in albums]
    df = pd.DataFrame(dic_flattened)

    # Save albums in Google BigQuery
    bigquery_client = bigquery.Client(project=project_id)
    dataset = bigquery_client.dataset(dataset_name)

    df_all_albums = df[['album_uri',
                         'added_at',
                         'album_artists_0_name',
                         'album_name',
                         'album_type' ,
                         'album_label',
                         'album_release_date',
                         'album_tracks_total',
                         'album_id']]

    #print(df_all_albums.head(10))
    gbq.to_gbq(df_all_albums, 'spotify.all_albums', project_id, if_exists='replace')

    return 'dataset loaded to BQ'

def upload_blob(bucket_name, blob_text, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(blob_text)

    print('File {} uploaded to {}.'.format(
        destination_blob_name,
        bucket))

    return "File uploaded to storage {}".format(destination_blob_name)


def get_recently_played(request):
    """Background Cloud Function fetch recently played tracks
       from Spotity account
    """
    import spotipy
    import spotipy.util as util
    from spotipy.oauth2 import SpotifyOAuth

    import json

    # Constants
    scope = 'user-read-recently-played'
    USER = 'SERGEBOU'
    bucket_name = "secret-compass-181513"

    token = util.prompt_for_user_token(USER, scope)

    if token:
       sp = spotipy.Spotify(auth=token)

       recentracks = sp.current_user_recently_played(limit=50)

    timestr = time.strftime("%Y%m%d_%H%M%S")
    folders = "{}/{}/{}".format(time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"))
    recent_tracks_json = json.dumps(recentracks, sort_keys=True, indent=2)

    print("spotify/{}/recently_played_{}.json".format(folders, timestr))

    upload_blob( bucket_name,
                 recent_tracks_json,
                 "spotify/{}/recently_played_{}.json".format(folders, timestr)
                )

    load_tracks_bq( recentracks )

    return recent_tracks_json

def get_recent_albums(request):
    """Background Cloud Function fetch recent albums from saved albums
       in Spotity account
    """
    import spotipy
    import spotipy.util as util
    from spotipy.oauth2 import SpotifyOAuth

    import json

    # Constants
    scope = 'user-library-read'
    USER = 'SERGEBOU'
    bucket_name = "secret-compass-181513"

    token = util.prompt_for_user_token(USER, scope)

    if token:
       sp = spotipy.Spotify(auth=token)

       recentalbums = sp.current_user_saved_albums(limit=50)

    timestr = time.strftime("%Y%m%d_%H%M%S")
    folders = "{}/{}/{}".format(time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"))
    recent_albums_json = json.dumps(recentalbums, sort_keys=True, indent=2)

    print("spotify/{}/recent_albums_{}.json".format(folders, timestr))
    #print(recentalbums["items"][0]["album"]["name"])
    upload_blob( bucket_name,
                 recent_albums_json,
                 "spotify/{}/recent_albums_{}.json".format(folders, timestr)
                )

    load_albums_bq(recentalbums)

    return recent_albums_json

def get_all_saved_albums(request):
    scope = 'user-library-read'
    albums = []

    token = util.prompt_for_user_token(USER, scope)

    if token:
       sp = spotipy.Spotify(auth=token)
       results = sp.current_user_saved_albums(limit=50)

    albums.extend(results['items'])
    while results['next']:
        results = sp.next(results)
        albums.extend(results['items'])

    timestr = time.strftime("%Y%m%d_%H%M%S")
    folders = "{}/{}/{}".format(time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"))
    all_albums_json = json.dumps(albums, sort_keys=True, indent=2)
    print("spotify/{}/all_albums_{}.json".format(folders, timestr))

    upload_blob( bucket_name,
                 all_albums_json,
                 "spotify/{}/all_albums_{}.json".format(folders, timestr)
                )

    load_all_albums_bq(albums)

    return "spotify/{}/all_albums_{}.json".format(folders, timestr)

if __name__ == '__main__':
    get_all_saved_albums(None)
    #recent_albums = get_recent_albums(None)
    #recent_tracks = get_recently_played(None)
