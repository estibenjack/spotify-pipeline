SELECT
  DATE(played_at) AS listen_date,
  ROUND(SUM(duration_s) / 60.0, 2) AS total_mins
FROM recently_played_tracks
GROUP BY 1
ORDER BY 1 DESC;