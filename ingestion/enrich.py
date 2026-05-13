import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def enrich_artist_data():
    load_dotenv()

    # db connection setup
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)

    api_key = os.getenv("LASTFM_API_KEY")
    base_url = "https://ws.audioscrobbler.com/2.0/"

    # query for artists missing from the bridge table
    query = """
    SELECT a.artist_id, a.artist_name 
    FROM dim_artists a
    LEFT JOIN bridge_artist_genres bag ON a.artist_id = bag.artist_id
    WHERE bag.artist_id IS NULL
    """

    with engine.connect() as conn:
        artists_to_enrich = pd.read_sql(text(query), conn)

    if artists_to_enrich.empty:
        print("All artists are already enriched! 🚀")
        return

    print(f"Enriching {len(artists_to_enrich)} artists via Last.fm API...")

    # enrichment loop
    for _, row in artists_to_enrich.iterrows():
        artist_id = row['artist_id']
        artist_name = row['artist_name'].strip()

        params = {
            "method": "artist.getinfo",
            "artist": artist_name,
            "api_key": api_key,
            "format": "json"
        }

        try:
            # forcing verify=False here to bypass mac SSL issue
            response = requests.get(
                base_url, params=params, verify=False, timeout=10)
            data = response.json()

            # navigate the Last.fm "Tags" JSON structure
            if 'artist' in data and 'tags' in data['artist']:
                tags = data['artist']['tags'].get('tag', [])

                # if there's only one tag, last.fm sometimes returns a dict instead of a list
                if isinstance(tags, dict):
                    tags = [tags]

                if tags:
                    found_genres = []
                    with engine.begin() as conn:
                        for t in tags[:5]:
                            g_name = t['name'].lower()

                            # insert into dim_genres
                            conn.execute(text(
                                "INSERT INTO dim_genres (genre_name) VALUES (:g) ON CONFLICT (genre_name) DO NOTHING"
                            ), {"g": g_name})

                            # get the genre_id
                            res = conn.execute(
                                text("SELECT genre_id FROM dim_genres WHERE genre_name = :g"), {"g": g_name})
                            g_id = res.scalar()

                            # insert into bridge table
                            conn.execute(text("""
                                INSERT INTO bridge_artist_genres (artist_id, genre_id) 
                                VALUES (:a_id, :g_id) 
                                ON CONFLICT DO NOTHING
                            """), {"a_id": artist_id, "g_id": g_id})

                            found_genres.append(g_name)

                    print(f"  ✅ {artist_name}: {found_genres}")
                else:
                    print(f"  ⚠️ No tags found for {artist_name}")
            else:
                print(f"  ❌ Artist not found on Last.fm: {artist_name}")

        except Exception as e:
            print(f"  🔥 Critical Error on {artist_name}: {e}")

    print("\nEnrichment process complete!")


if __name__ == "__main__":
    enrich_artist_data()
