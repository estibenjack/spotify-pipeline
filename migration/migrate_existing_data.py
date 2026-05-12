import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine


def migrate_existing_data():
    load_dotenv()

    # supabase db variables
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")

    conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(conn_str)

    # start extract - grab all raw data from old table
    df = pd.read_sql("SELECT * FROM recently_played_tracks", engine)

    # transform tracks - prep the track metadata
    dim_tracks = df[['track_id', 'track_name', 'album', 'duration_s']].copy()
    # putting duration back into ms from s (matching the spotify API standard)
    dim_tracks['duration_ms'] = dim_tracks['duration_s'] * 1000
    # rename 'album' to match the new sql schema and drop the old secs col
    dim_tracks = dim_tracks.rename(
        columns={'album': 'album_name'}).drop(columns=['duration_s'])
    # make sure there's only one row per unique track
    dim_tracks = dim_tracks.drop_duplicates(subset=['track_id'])

    # transform artists -> fixing the multiple artist problem here
    # i'm splitting string like 'theodora, jeez suave' into actual lists then exploding them
    # so every artist gets their own row - this'll let me link tracks to multiple artists
    exploded_df = df.assign(
        artist=df['artist'].str.split(',')).explode('artist')
    exploded_df['artist'] = exploded_df['artist'].str.strip()

    dim_artists = exploded_df[['artist']].drop_duplicates().rename(
        columns={'artist': 'artist_name'})

    # using the artist_name as the id for now until i pull spotify IDs in the enrichment layer
    dim_artists['artist_id'] = dim_artists['artist_name']

    # transform bridge - mapping tracks to their artist(s)
    # the bridge table allows for many-to-many r/ships
    bridge_track_artists = exploded_df[[
        'track_id', 'artist']].drop_duplicates()
    bridge_track_artists = bridge_track_artists.rename(
        columns={'artist': 'artist_id'})

    # transform fact - prepping the listening history
    fact_df = df[['played_at', 'track_id']].copy()

    # --- LOAD: sending data to supabase (order is essential here!!) ---
    # loading dims first, then bridge/fact tables
    # this avoids FK errors (you can't link to a track if it doesn't exist yet)
    print("Loading Dimensions...")
    dim_tracks.to_sql("dim_tracks", engine, if_exists='append', index=False)
    dim_artists.to_sql("dim_artists", engine, if_exists='append', index=False)

    print("Loading Bridge...")
    bridge_track_artists.to_sql(
        "bridge_track_artists", engine, if_exists='append', index=False)

    print("Loading Fact Table...")
    fact_df.to_sql("fact_recently_played", engine,
                   if_exists='append', index=False)

    print("Migration Successful!")


if __name__ == "__main__":
    migrate_existing_data()
