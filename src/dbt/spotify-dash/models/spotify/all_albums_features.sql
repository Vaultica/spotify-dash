{{ config(materialized='view') }}

with source_data as (

  select
     added_at,
     album_artists_0_name	                                as album_artist_name,
     albums.album_name,
     album_type,
     album_label,
     album_release_date,
     album_tracks_total,
     round(play.no_albums / album_tracks_total, 1)        as album_plays,
     concat('https://open.spotify.com/album/' , album_id) as album_url,
  from {{ source('spotify', 'all_albums') }} albums
  left join {{ ref('spotify', 'most_played_albums') }} play
       on play.album_name = albums.album_name

)

select
     *
from source_data
