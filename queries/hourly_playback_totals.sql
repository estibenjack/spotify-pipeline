SELECT 
  EXTRACT(HOUR FROM played_at) AS hour_of_day, 
  COUNT(*) AS play_count
FROM recently_played_tracks
GROUP BY 1
ORDER BY 1 ASC;