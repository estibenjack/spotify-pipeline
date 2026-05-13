import os
import psycopg2
from dotenv import load_dotenv


def load_tracks(transformed_data):
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    try:
        # load artists
        for artist in transformed_data['artists']:
            cur.execute("""
                INSERT INTO dim_artists (artist_id, artist_name)
                VALUES (%s, %s) ON CONFLICT (artist_id) DO NOTHING
            """, (artist['artist_id'], artist['artist_name']))

        # load tracks
        for track in transformed_data['tracks']:
            cur.execute("""
                INSERT INTO dim_tracks (track_id, track_name, album_name, duration_ms)
                VALUES (%s, %s, %s, %s) ON CONFLICT (track_id) DO NOTHING
            """, (track['track_id'], track['track_name'], track['album_name'], track['duration_ms']))

        # load bridge (track-artist links)
        for link in transformed_data['track_artist_links']:
            cur.execute("""
                INSERT INTO bridge_track_artists (track_id, artist_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (link['track_id'], link['artist_id']))

        # load recent plays fact table
        for play in transformed_data['plays']:
            cur.execute("""
                INSERT INTO fact_recently_played (track_id, played_at)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (play['track_id'], play['played_at']))

        conn.commit()
        print("Successfully loaded all normalised data.")
    except Exception as e:
        conn.rollback()
        print(f"Error during load: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    from ingestion.extract import get_recently_played
    from transformation.transform import transform_tracks
    results = get_recently_played()
    transformed_tracks = transform_tracks(results)
    load_tracks(transformed_tracks)
