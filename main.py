import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from sqlalchemy import create_engine
from keys.keys import *
from queries import Query
from datetime import datetime
import argparse
import os
cwd = os.getcwd()

# Replace these with your own credentials
client_id = CLIENT_ID
client_secret = CLIENT_SECRET
engine = create_engine('sqlite:///gig_spotify.db',echo=True)

# Set up the client credentials manager
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

class SetupBaseTables:

    # Constructor
    def __init__(self, get_playlists = False, get_artists = False, get_features = False):
        self.get_pl = get_playlists
        self.get_ar = get_artists
        self.get_features = get_features
        self.ccm = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=self.ccm)

    # Function to create the country table
    def create_country_table(self):
        path = os.getcwd()+"/data/countries.csv"
        df = pd.read_csv(path)
        df.to_sql('countries',con=engine,if_exists='replace',index=False)

    # Function to get the top playlists for each country
    def get_country_top_playlist(self,viral = False):
        playlist_list = []
        if viral:
            focus = "Viral 50 - "
        else:
            focus = "Top 50 - "
        df = pd.read_sql_query(f'SELECT * FROM countries', engine)
        for country in df["country"]:
            playlist_search = focus+country
            playlists = self.sp.search(q=playlist_search, type='playlist', limit=5)
            for i in range(len(playlists)):
                try:
                    owner = playlists["playlists"]["items"][i]["owner"]["display_name"]
                    if owner != "Spotify":
                        continue
                    name = playlists["playlists"]["items"][i]["name"]
                    id = playlists["playlists"]["items"][i]["id"]
                    owner = playlists["playlists"]["items"][i]["owner"]["display_name"]
                    dict_playlist = {"name": name, "playlist_id": id, "owner": owner}
                    playlist_list.append(dict_playlist)
                except Exception as e:
                    print(f"Country: "+country+" had an error, {e}.")
        return playlist_list

    # Function to filter top and viral playlists
    def filter_top_viral(self,df):
        mask = df["name"].str.contains("Top 50 -|Viral 50 -")
        return df.loc[mask]

    # Function to get playlist tracks
    def get_playlist_tracks(self):
        df = pd.read_sql_query(Query.query_playlists, engine)
        list_of_dicts = []
        for i in range(len(df)):
            id = df["playlist_id"].iloc[i]
            pl = df["name"].iloc[i]
            res2 = self.sp.playlist_tracks(id)
            number_of_songs = len(res2["items"])
            for i in range(number_of_songs):
                if res2["items"][i]["track"] is None:
                    pass
                else:
                    artist_name = res2["items"][i]["track"]["album"]["artists"][0]["name"]
                    artist_id = res2["items"][i]["track"]["album"]["artists"][0]["id"]
                    track_name = res2["items"][i]["track"]["name"]
                    track_id = res2["items"][i]["track"]["id"]
                    release_date = res2["items"][i]["track"]["album"]["release_date"]
                    duration = res2["items"][i]["track"]["duration_ms"]
                    popularity = res2["items"][i]["track"]["popularity"]
                    dict = {"artist": artist_name, "artist_id": artist_id, "track_name": track_name, "track_id" : track_id,  "popularity": popularity,"release_date": release_date,"duration": duration, "playlist": pl}
                    list_of_dicts.append(dict)

        df_spotify = pd.DataFrame(list_of_dicts)
        df_spotify["upload_time"]  = datetime.utcnow()
        return df_spotify
    
    # Function to get artist information
    def get_artist_info(self):
        list_of_dicts = []
        df = pd.read_sql_query(Query.query_artists, engine)
        artist_ids = df["artist_id"].to_list()
        for id in artist_ids:
            artist_info = self.sp.artist(id)
            name = artist_info["name"]
            num_followers = artist_info["followers"]["total"]
            genres = artist_info["genres"]
            popularity = artist_info["popularity"]
            dict = {"name": name, "artist_id": id, "num_followers": num_followers, "genres" : genres, "popularity": popularity}
            list_of_dicts.append(dict)

        df_artists = pd.DataFrame(list_of_dicts)
        df_artists["upload_time"]  = datetime.utcnow()
        return df_artists
    
    # Function to get track audio features
    def get_track_audio_features(self):
        list_of_dicts = []
        df = pd.read_sql_query(Query.query_tracks, engine)
        track_ids = df["track_id"].to_list()
        batch_size = 50
        for i in range(0,len(track_ids),batch_size):
            track_batch = track_ids[i:i + batch_size]
            track_info = self.sp.audio_features(track_batch)
            for i in range(len(track_info)):
                danceability = track_info[i]["danceability"] # How suitable a track is from 0-1 with 1 being highest.
                track_id = track_info[i]["id"]
                energy = track_info[i]["energy"] # How energetic a track is from 0-1 with 1 being the highest.
                mode = track_info[i]["mode"] # If it is a major or minor mode, 0 is minor 1 is major
                speechiness = track_info[i]["speechiness"] # How speechy the track is. A speech or podcast would be close to 1.
                acousticness = track_info[i]["acousticness"]
                instrumentalness = track_info[i]["instrumentalness"] # How instrumental a track is from 0-1.
                liveness = track_info[i]["liveness"] # Detects if the track is a live recording
                valence = track_info[i]["valence"] # How happy music sounds from 0-1 with 1 being highest.
                tempo = track_info[i]["tempo"] # BMP.
                key = track_info[i]["key"] # Which music key, 0=C 1=C#, 2=D e.t.c
                dict = {"track_id": track_id, 
                        "danceability": danceability,  
                        "energy" : energy,  
                        "mode" : mode,  
                        "speechiness" : speechiness,  
                        "acousticness" : acousticness,  
                        "instrumentalness" : instrumentalness,  
                        "liveness" : liveness,  
                        "valence" : valence,
                        "tempo" : tempo,  
                        "key" : key
                        
                        }
                list_of_dicts.append(dict)

        df_tracks = pd.DataFrame(list_of_dicts)
        df_tracks["upload_time"]  = datetime.utcnow()
        return df_tracks
    
    # Function to push data to SQLite
    def push_to_sql(self,df, table_name, if_exists):
        df.to_sql(table_name,con=engine,if_exists=if_exists,index=False)

    # Function to run the initial setup
    def run_setup(self):
        playlist_top = self.get_country_top_playlist(viral=False)
        playlist_viral = self.get_country_top_playlist(viral=True)  
        all_playlists = playlist_viral+playlist_top
        df_all = pd.DataFrame(all_playlists)
        df = self.filter_top_viral(df_all)
        self.push_to_sql(df,table_name="countries",if_exists="replace")

    # Function to run playlist data extraction
    def run_playlists(self):
        df = self.get_playlist_tracks()
        self.push_to_sql(df=df,table_name="playlist_tracks",if_exists="append")

    # Function to run artist data extraction
    def run_artists(self):
        df = self.get_artist_info()
        self.push_to_sql(df,table_name="artists",if_exists="append")
    
    # Function to run audio features extraction
    def run_features(self):
        df = self.get_track_audio_features()
        self.push_to_sql(df,table_name="audio_features",if_exists="append")

    # Function to run the script
    def run(self):
        if self.get_pl:
            self.run_playlists()
        elif self.get_ar:
            self.run_artists()
        elif self.get_features:
            self.run_features()
        else:
            print("No action to be preformed.")
        print("Done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script which gets data from the Spotify API.')
    parser.add_argument('arg', type=str, help='Can be either p or a, and sets the value of get_artists and get_playlists')
    args = parser.parse_args()
    if args.arg == "p":
        sbt = SetupBaseTables(get_playlists=True)
        sbt.run()
    elif args.arg == "a":
        sbt = SetupBaseTables(get_artists=True)
        sbt.run()
    elif args.arg == "f":
        sbt = SetupBaseTables(get_features=True)
        sbt.run()