{{ config(materialized='view') }}

with source_data as (

  select
     track_name,
     played_at,
     track_album_artists_0_name,
     track_album_name,
     track_type,
     track_uri
  from {{ source('spotify', 'play_history_new') }}
  where track_name != 'href'
  group by 1,2,3,4,5,6
  order by played_at desc

)

select
       *
from source_data
