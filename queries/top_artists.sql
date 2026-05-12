WITH cleaned_data AS (
    SELECT TRIM(UNNEST(STRING_TO_ARRAY(artist, ','))) AS artist_name
    FROM recently_played_tracks
)

SELECT artist_name, COUNT(*) as plays
FROM cleaned_data
GROUP BY artist_name
ORDER BY plays DESC
LIMIT 5;