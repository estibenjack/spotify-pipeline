CREATE TABLE IF NOT EXISTS dim_artists (
  artist_id VARCHAR(50) PRIMARY KEY,
  artist_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_tracks (
  track_id VARCHAR(50) PRIMARY KEY,
  track_name TEXT NOT NULL,
  album_name TEXT,
  duration_ms INTEGER
);

CREATE TABLE IF NOT EXISTS bridge_track_artists (
  track_id VARCHAR(50) REFERENCES dim_tracks(track_id),
  artist_id VARCHAR(50) REFERENCES dim_artists(artist_id),
  PRIMARY KEY (track_id, artist_id)
);

CREATE TABLE IF NOT EXISTS dim_genres (
  genre_id SERIAL PRIMARY KEY,
  genre_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS bridge_artist_genres (
  artist_id VARCHAR(50) REFERENCES dim_artists(artist_id),
  genre_id INTEGER REFERENCES dim_genres(genre_id),
  PRIMARY KEY (artist_id, genre_id)
);

CREATE TABLE IF NOT EXISTS fact_recently_played (
  play_id SERIAL PRIMARY KEY,
  played_at TIMESTAMP WITH TIME ZONE NOT NULL,
  track_id VARCHAR(50) REFERENCES dim_tracks(track_id)
);
