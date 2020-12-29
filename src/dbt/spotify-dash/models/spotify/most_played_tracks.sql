{{ config(materialized='view') }}

with source_data as (

  select
      count(track_name) no_tracks,
      track_name
  FROM {{ ref('spotify', 'playback_history') }}
  group by track_name
)

select  *
from source_data
order by no_tracks desc
