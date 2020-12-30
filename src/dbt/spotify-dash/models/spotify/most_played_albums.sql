{{ config(materialized='view') }}

with source_data as (

  select
      count(track_album_name) no_albums,
      track_album_name as album_name
  FROM {{ ref('spotify', 'playback_history') }}
  group by track_album_name
)

select
     *
from source_data
order by no_albums desc
