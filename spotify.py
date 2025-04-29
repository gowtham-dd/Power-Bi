from spotipy.oauth2 import SpotifyClientCredentials
import spotipy 
import pandas as pd
import matplotlib.pyplot as plt
import re
import mysql.connector

# Spotify API authentication
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id='',
    client_secret=''
))

# TiDB database connection config
db_config = {
    'host': "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    'port': 4000,
    'user': "4PFhDQQc2yhfCUH.root",
    'password': "tzNB9UAMnXNidi62",
    'database': "sampleTable",
}

# Connect to the TiDB database
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Read track URLs from file
file_path = "track_urls.txt"
with open(file_path, 'r') as file:
    track_urls = file.readlines()

# Process each URL
for track_url in track_urls:
    track_url = track_url.strip()
    try:
        track_id = re.search(r'track/([a-zA-Z0-9]+)', track_url).group(1)
        track = sp.track(track_id)

        track_data = {
            'Track Name': track['name'],
            'Artist': track['artists'][0]['name'],
            'Album': track['album']['name'],
            'Popularity': track['popularity'],
            'Duration (minutes)': round(track['duration_ms'] / 60000, 2)
        }

        insert_query = """
        INSERT INTO spotify_tracks_analysis (track_name, artist, album, popularity, duration_minutes)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            track_data['Track Name'],
            track_data['Artist'],
            track_data['Album'],
            track_data['Popularity'],
            track_data['Duration (minutes)']
        ))
        connection.commit()
        print(f"Inserted: {track_data['Track Name']} by {track_data['Artist']}")

    except Exception as e:
        print(f"Error processing {track_url}: {e}")

# Close DB connection
cursor.close()
connection.close()

print("All tracks processed.")
