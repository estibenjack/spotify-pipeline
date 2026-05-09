-- create the recently played tracks table if it doesn't already exist
CREATE TABLE IF NOT EXISTS recently_played_tracks (
  -- auto-generated primary key
  id SERIAL PRIMARY KEY,
  -- spotify's unique track identifier
  track_id VARCHAR(50),
  -- name of the track
  track_name TEXT,
  -- artist name(s) - multiple artists will be joined as a single, comma-separated string
  artist TEXT,
  -- album name
  album TEXT,
  -- track duration in seconds (converted from milliseconds in transform.py)
  duration_s INTEGER,
  -- timestamp of when the track was played - unique to prevent duplicate entries
  played_at TIMESTAMP WITH TIME ZONE UNIQUE
);
