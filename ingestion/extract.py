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
    spotify_refresh_token = os.getenv("SPOTIFY_REFRESH_TOKEN")

    scope = "user-read-recently-played"
    auth_manager = SpotifyOAuth(client_id=spotify_client_id,
                                client_secret=spotify_client_secret,
                                redirect_uri=spotify_redirect_uri,
                                scope=scope)

    # use refresh token to get a new access token without browser login
    token_info = auth_manager.refresh_access_token(spotify_refresh_token)

    sp = spotipy.Spotify(auth=token_info['access_token'])

    # fetch 50 most recently played tracks
    results = sp.current_user_recently_played(limit=50)

    return results


if __name__ == "__main__":
    results = get_recently_played()
    print(results)
