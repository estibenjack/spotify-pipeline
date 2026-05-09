from ingestion.extract import get_recently_played
from transformation.transform import transform_tracks
from loading.load import load_tracks
from datetime import datetime

if __name__ == "__main__":
    print(f"Pipeline starting - {datetime.now()}")
    raw_results = get_recently_played()
    tracks_to_load = transform_tracks(raw_results)
    load_tracks(tracks_to_load)
    print(f"Pipeline complete - {datetime.now()}")
