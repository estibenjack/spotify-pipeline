def transform_tracks(results):
    all_tracks = []

    for item in results['items']:
        track_id = item['track']['id']
        track_name = item['track']['name']
        # handle the artists list — join multiple artists into one comma separated string
        artist = ', '.join([artist['name']
                           for artist in item['track']['artists']])
        album = item['track']['album']['name']
        # spotify returns duration in milliseconds — dividing by 1000 and casting to int to get clean seconds
        duration_s = int(item['track']['duration_ms'] / 1000)
        played_at = item['played_at']
        # append as a list — order matches the column order in schema.sql
        all_tracks.append([track_id, track_name, artist,
                          album, duration_s, played_at])

    return all_tracks


if __name__ == "__main__":
    from ingestion.extract import get_recently_played
    results = get_recently_played()
    all_tracks = transform_tracks(results)
    for idx, track in enumerate(all_tracks):
        print(idx, track)
