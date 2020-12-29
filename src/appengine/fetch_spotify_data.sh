#!/bin/bash

source /Users/sergeboo/.bash_profile

SPOTIPY_CLIENT_ID='d2c92ddc265d4663a323756acffa52ba'
SPOTIPY_CLIENT_SECRET='c2e82e806fc844cbb89654572060db8a'
SPOTIPY_REDIRECT_URI='http://localhost:8080/callback'
GOOGLE_APPLICATION_CREDENTIALS=/Users/sergeboo/Projects/spotify/cpb100-267aa5b92aa7.json

python /Users/sergeboo/Projects/spotify/fetch_spotify_data.py >> /Users/sergeboo/Projects/spotify/fetch_spotify_data.log 2>&1



