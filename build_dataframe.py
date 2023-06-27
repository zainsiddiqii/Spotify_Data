import json
import pandas as pd
import datetime
from pathlib import Path

class BuildDataFrame:
    """This class will create a dataframe from the json files."""

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

        # clean values in 'platform' column
        self.dataframe.loc[self.dataframe["platform"].str.contains("web_player", case=False), "platform"] = "browser"
        self.dataframe.loc[self.dataframe["platform"].str.contains("iOS", case=False), "platform"] = "ios"
        self.dataframe.loc[self.dataframe["platform"].str.contains("Windows", case=False), "platform"] = "windows"
        self.dataframe.loc[self.dataframe["platform"].str.contains("ps4", case=False), "platform"] = "ps4"
        self.dataframe.loc[self.dataframe["platform"].str.contains("tizen", case=False), "platform"] = "tv"
        print("Cleaned values in platform column (e.g iOS 11.0.1 (iPhone9,3) -> ios)...")

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

            return self.dataframe.sample(5)