import json
import pandas as pd
import datetime
import spotipy
import pandas as pd
from pathlib import Path
from spotipy.oauth2 import SpotifyClientCredentials

class ExtendedStreamingHistory:
    """This class will create a dataframe from the extended streaming history json files provided by spotify."""

    def __init__(self, number_of_files: int):

        self.number_of_files = number_of_files
        self.dataframe = None
        self.pii_columns = ["ip_addr_decrypted",
                            "user_agent_decrypted",
                            "conn_country"]
        self.columns_to_drop = ["username",
                                "episode_name",
                                "episode_show_name",
                                "spotify_episode_uri",
                                "incognito_mode",
                                "offline_timestamp",
                                "ms_played",
                                "ts",
                                "skipped"]
        self.columns_to_rename = {"master_metadata_track_name": "track_name",
                                  "master_metadata_album_artist_name": "artist_name",
                                  "master_metadata_album_album_name": "album_name",
                                  "spotify_track_uri": "track_uri"}

    def create_dataframe(self) -> pd.DataFrame:
        """This function will create a dataframe from the json files."""

        data = {}

        # read the files and store the pandas DataFrames in the dictionary
        for i in range(self.number_of_files):
                with open(f"spotify_data/endsong_{i}.json", encoding = "utf8") as file:
                     content = json.load(file)
                     data[f"data_{i}"] = pd.DataFrame(content)

        self.dataframe = pd.concat((dataset for dataset in data.values()))

        # drop columns with sensitive information
        self.dataframe.drop(columns = self.pii_columns,
                        inplace=True)
        print(f"Dropped columns {', '.join(self.pii_columns)} containing sensitive information...")
        print("The columns in this dataset are: \n", self.dataframe.columns)
        
        return self.dataframe
            
    def fix_columns(self):
        """This function will fix the columns and make them appropriate for analysis."""

        print("Fixing columns...")
        # convert 'ts' column to datetime format and remove timezone
        self.dataframe["time_ended"] = pd.to_datetime(self.dataframe["ts"], format="ISO8601")
        self.dataframe["time_ended"] = self.dataframe["time_ended"].dt.tz_localize(None)
        print("Created time_ended column in datetime format...")

        # Convert 'ms_played' column to hh:mm:ss format by creating stream_duration column
        total_sec = self.dataframe["ms_played"] / 1000
        self.dataframe["stream_duration"] = total_sec.apply(lambda x: datetime.timedelta(seconds = x))
        self.dataframe["stream_duration"] = self.dataframe["stream_duration"].astype("timedelta64[s]")
        print("Created stream_duration column in timedelta format...")

        #create time_started column and convert it to datetime format
        self.dataframe["time_started"] = self.dataframe["time_ended"] - self.dataframe["stream_duration"]
        print("Created time_started column in datetime format...")

        #clean values in 'platform' column
        self.dataframe.loc[self.dataframe["platform"].str.contains("web_player", case=False), "platform"] = "browser"
        self.dataframe.loc[self.dataframe["platform"].str.contains("iOS", case=False), "platform"] = "ios"
        self.dataframe.loc[self.dataframe["platform"].str.contains("Windows", case=False), "platform"] = "windows"
        self.dataframe.loc[self.dataframe["platform"].str.contains("ps4", case=False), "platform"] = "ps4"
        self.dataframe.loc[self.dataframe["platform"].str.contains("tizen", case=False), "platform"] = "tv"
        print("Cleaned values in platform column (e.g iOS 11.0.1 (iPhone9,3) -> ios)...")

        #clean vlues in spotify_track_uri column
        self.dataframe["spotify_track_uri"] = self.dataframe["spotify_track_uri"].str.split(":", expand = True)[2]
        self.dataframe = self.dataframe[self.dataframe["spotify_track_uri"].notna()]
        print("Cleaned values in spotify_track_uri column (e.g spotify:track:{actual id} -> actual id)...")
        
        # drop rows where something was streamed for less than 10 seconds
        self.dataframe = self.dataframe[self.dataframe["ms_played"] > 999.9 * 10]
        print("Dropped rows where something was streamed for less than 10 seconds...")

    def drop_columns(self, columns: list) -> pd.DataFrame:
        """This function will drop the columns that are not needed for the analysis."""

        self.dataframe.drop(columns = columns, inplace=True)
        print(f"Dropped columns {', '.join(self.columns_to_drop)}")
    
    def rename_columns(self, columns: dict) -> pd.DataFrame:
        """This function will rename the columns."""

        self.dataframe.rename(columns = columns, inplace=True)
        for key, value in self.columns_to_rename.items():
             print(f"Renamed column {key} to {value}.")
    
    def to_csv(self, filepath: str) -> None:
        """This function will save the dataframe to a csv file."""

        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.dataframe.to_csv(path, index=False)
        print(f"Saved dataframe as csv file to {path}")

    def preview_dataframe(self) -> pd.DataFrame:
        """This function will preview the dataframe by selecting 5 random rows."""
        sample = self.dataframe.sample(5)
        print(sample)

class AudioFeatures:
    """This class will build a dataframe from the audio features json files obtained from
      the spotify API. Users can save the spotify uris from their extended streaming history
      and use them to extract the audio features for the songs they listened to."""
    
    def __init__(self, spotify_client_id: str, spotify_client_secret: str):
        
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.client_credentials_manager = SpotifyClientCredentials(client_id = self.spotify_client_id,
                                                                client_secret = self.spotify_client_secret)
        self.spotify = spotipy.Spotify(client_credentials_manager = self.client_credentials_manager)
            
    def create_uri_list(self, filepath: str) -> list:
        """This function will create a list of track uris from the csv file containing the track uris.
        We will create a list of lists of 100 track uris each. This is because the spotify API can only
        extract audio features for 100 songs at a time."""

        self.extended_streaming_history = pd.read_csv(filepath)
        self.track_uris = self.extended_streaming_history["track_uri"].unique()
        print(f"Extracted track uris from {filepath}...")
        self.track_uris = self. track_uris.tolist()
        self.track_uris_list = [self.track_uris[i:i + 100] for i in range(0, len(self.track_uris), 100)]
        self.columns_to_drop = ["type",
                                "id",
                                "uri",
                                "track_href",
                                "analysis_url",
                                "duration_ms"]
        
        print("Created list of lists of 100 track uris each...")
        print("Number of unique track uris: ", len(self.track_uris))

        return self.track_uris
    
    def extract_audio_features(self) -> pd.DataFrame:
        """This function will extract the audio features for the track uris.
        It then places the audio features in a dataframe."""

        self.audio_features_data = []

        for i, track_uris_list in enumerate(self.track_uris_list):
              print(f"Extracting audio features for list {i}...")
              features = self.spotify.audio_features(track_uris_list)
              
              for feature in features:
                   self.audio_features_data.append({'track_uri': feature['id'], **feature})

        print("Extracted audio features for all track uris...")
        self.dataframe = pd.DataFrame(self.audio_features_data)
        print("Created dataframe from audio features data...")
        total_sec = self.dataframe["duration_ms"] / 1000
        self.dataframe["song_length"] = total_sec.apply(lambda x: datetime.timedelta(seconds = x))
        self.dataframe["song_length"] = self.dataframe["song_length"].astype("timedelta64[s]")
        print("Created song_duration column in timedelta format...")

        self.dataframe.drop(columns = self.columns_to_drop, inplace = True)
        print(f"Dropped columns {', '.join(self.columns_to_drop)}...")

        return self.dataframe
    
    def to_csv(self, filepath: str) -> None:
        """This function will save the dataframe to a csv file."""

        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.dataframe.to_csv(path, index=False)
        print(f"Saved dataframe as csv file to {path}")

    def preview_dataframe(self) -> pd.DataFrame:
        """This function will preview the dataframe by selecting 5 random rows."""

        sample = self.dataframe.sample(5)
        print(sample)

        return sample
    
class FullDataset:
    """This class will combine the extended streaming history and audio features data 
    to create a full dataset."""

    def __init__(self, extended_streaming_history_filepath: str, audio_features_filepath: str):
           
        self.extended_streaming_history_filepath = extended_streaming_history_filepath
        self.audio_features_filepath = audio_features_filepath
        self.extended_streaming_history = None
        self.audio_features = None
        self.full_dataset = None
                
    def create_full_dataset(self) -> pd.DataFrame:
        """This function will create the full dataset by merging the extended streaming history
                and audio features data."""
        
        self.extended_streaming_history = pd.read_csv(self.extended_streaming_history_filepath)
        self.audio_features = pd.read_csv(self.audio_features_filepath)
        self.dataframe = pd.merge(self.extended_streaming_history, self.audio_features, on = "track_uri")
        print("Created full dataset...")
        
        return self.dataframe
    
    def to_csv(self, filepath: str) -> None:
        """This function will save the dataframe to a csv file."""
        
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.dataframe.to_csv(path, index=False)
        print(f"Saved dataframe as csv file to {path}")

    def preview_dataframe(self) -> pd.DataFrame:
        """This function will preview the dataframe by selecting 5 random rows."""

        sample = self.dataframe.sample(5)
        print(sample)
        
        return sample