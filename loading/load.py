import os
from dotenv import load_dotenv
import psycopg2


def load_tracks(tracks):
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")

    print(f"Connecting to database -> '{db_name}' on PORT {db_port}")

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )

    cur = conn.cursor()

    attempts_count = 0
    inserted_rows = 0
    # if a listened-to track is already in the db, do nothing (ON CONFLICT (played_at) DO NOTHING)
    for track in tracks:
        attempts_count += 1
        cur.execute(
            """
            INSERT INTO recently_played_tracks (track_id, track_name, artist, album, duration_s, played_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (played_at) DO NOTHING
            """,
            track
        )
        inserted_rows += cur.rowcount
    print(f"{attempts_count} row(s) attempted, {inserted_rows} row(s) inserted, {attempts_count-inserted_rows} row(s) skipped")
    # commit once after all inserts — more efficient than committing inside the loop
    conn.commit()

    print("The load is complete!")

    cur.close()
    conn.close()


if __name__ == "__main__":
    from ingestion.extract import get_recently_played
    from transformation.transform import transform_tracks
    results = get_recently_played()
    transformed_tracks = transform_tracks(results)
    load_tracks(transformed_tracks)
