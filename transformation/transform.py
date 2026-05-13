def transform_tracks(results):
    transformed_data = {
        'artists': [],
        'tracks': [],
        'track_artist_links': [],
        'plays': []
    }

    for item in results['items']:
        track = item['track']
        track_id = track['id']
        played_at = item['played_at']

        # prepare track data
        transformed_data['tracks'].append({
            'track_id': track_id,
            'track_name': track['name'],
            'album_name': track['album']['name'],
            'duration_ms': track['duration_ms']
        })

        # prepare artist data and links
        for artist in track['artists']:
            artist_id = artist['id']
            transformed_data['artists'].append({
                'artist_id': artist_id,
                'artist_name': artist['name']
            })

            # link for the bridge table
            transformed_data['track_artist_links'].append({
                'track_id': track_id,
                'artist_id': artist_id
            })

        # prepare fact table data (plays)
        transformed_data['plays'].append({
            'track_id': track_id,
            'played_at': played_at
        })

    # remove duplicates from dimension lists before returning
    transformed_data['artists'] = [dict(t) for t in {tuple(
        d.items()) for d in transformed_data['artists']}]
    transformed_data['tracks'] = [dict(t) for t in {tuple(
        d.items()) for d in transformed_data['tracks']}]
    transformed_data['track_artist_links'] = [dict(t) for t in {tuple(
        d.items()) for d in transformed_data['track_artist_links']}]

    return transformed_data


if __name__ == "__main__":
    from ingestion.extract import get_recently_played
    results = get_recently_played()
    all_tracks = transform_tracks(results)
    for idx, track in enumerate(all_tracks):
        print(idx, track)
