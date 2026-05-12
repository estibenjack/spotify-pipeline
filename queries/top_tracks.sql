SELECT
  track_name,
  artist,
  COUNT(*) AS play_count
FROM recently_played_tracks
GROUP BY track_id, track_name, artist
ORDER BY play_count DESC
LIMIT 5;