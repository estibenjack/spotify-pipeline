import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth


def get_recently_played():
    # load environment variables from .env
    load_dotenv()

    spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
    spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    spotify_redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")

    # authenticate with spotify using OAuth and request recently played scope
    scope = "user-read-recently-played"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                   client_secret=spotify_client_secret,
                                                   redirect_uri=spotify_redirect_uri,
                                                   scope=scope))

    # fetch 50 most recently played tracks
    results = sp.current_user_recently_played(limit=50)

    return results


if __name__ == "__main__":
    results = get_recently_played()
    print(results)
