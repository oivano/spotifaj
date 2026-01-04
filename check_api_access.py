#!/usr/bin/env python3
"""
Check Spotify API access levels
"""
import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

load_dotenv()

client_id = os.getenv('SPOTIPY_CLIENT_ID')
client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
username = os.getenv('SPOTIPY_USERNAME')

print("🔍 Checking Spotify API Access...\n")
print(f"Client ID: {client_id[:10]}...{client_id[-4:]}")
print(f"Username: {username}\n")

# Test with user authentication
scope = "user-read-private playlist-read-private"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=os.getenv('SPOTIPY_REDIRECT_URI'),
    scope=scope,
    username=username
))

print("Testing API endpoints:\n")

# Test 1: Basic user profile (should always work)
try:
    user = sp.current_user()
    print("✅ User Profile: OK")
except Exception as e:
    print(f"❌ User Profile: FAILED - {e}")

# Test 2: Search (should always work)
try:
    results = sp.search(q="test", type="track", limit=1)
    print("✅ Search: OK")
except Exception as e:
    print(f"❌ Search: FAILED - {e}")

# Test 3: Audio Features (blocked for you)
try:
    # Use a known track ID (Bohemian Rhapsody)
    features = sp.audio_features(["3z8h0TU7ReDPLIbEnYhWZb"])
    if features and features[0]:
        print("✅ Audio Features: OK")
    else:
        print("⚠️  Audio Features: Returns empty")
except Exception as e:
    print(f"❌ Audio Features: BLOCKED - {str(e)[:100]}")

# Test 4: Recommendations (blocked for you)
try:
    recs = sp.recommendations(seed_tracks=["3z8h0TU7ReDPLIbEnYhWZb"], limit=5)
    if recs and 'tracks' in recs:
        print("✅ Recommendations: OK")
    else:
        print("⚠️  Recommendations: Returns empty")
except Exception as e:
    print(f"❌ Recommendations: BLOCKED - {str(e)[:100]}")

# Test 5: Get Artist
try:
    artist = sp.artist("3TV7tssuSl8x7ARqsTvIyM")
    if artist:
        print("✅ Artist Info: OK")
except Exception as e:
    print(f"❌ Artist Info: FAILED - {e}")

print("\n" + "="*50)
print("📊 Summary:")
print("="*50)
print("\nIf Audio Features or Recommendations are blocked:")
print("→ Your app is in Development Mode with restricted API access")
print("→ Request Extended Quota Mode in Spotify Developer Dashboard")
print("→ Or use the fallback features (already working!)")
print("\n🔗 Spotify Developer Dashboard:")
print("   https://developer.spotify.com/dashboard")
