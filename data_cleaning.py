import glob
import os
from dotenv import load_dotenv
from build_datasets import ExtendedStreamingHistory, AudioFeatures, FullDataset

load_dotenv()

# specify spotify client credentials for authentication
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# count the number of files that match the pattern for files with song streaming history
NUMBER_OF_FILES = len(glob.glob("spotify_data/endsong_*.json"))

# specify filepaths to save the cleaned data
EXTENDED_STREAMING_HISTORY_FILEPATH = "cleaned_data/extended_streaming_history.csv"
AUDIO_FEATURES_FILEPATH = "cleaned_data/audio_features.csv"
FULL_DATASET_FILEPATH = "cleaned_data/full_dataset.csv"

extended_streaming_history = ExtendedStreamingHistory(NUMBER_OF_FILES)

extended_streaming_history.create_dataframe()
print("-"*50)
extended_streaming_history.preview_dataframe()
print("-"*50)
extended_streaming_history.fix_columns()
print("-"*50)
extended_streaming_history.drop_columns(extended_streaming_history.columns_to_drop)
print("-"*50)
extended_streaming_history.rename_columns(extended_streaming_history.columns_to_rename)
print("-"*50)
extended_streaming_history.to_csv(EXTENDED_STREAMING_HISTORY_FILEPATH)
print("-"*50)
extended_streaming_history.preview_dataframe()
print("-"*50, "\n", "-"*50, "\n", "-"*50)

audio_features = AudioFeatures(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
audio_features.create_uri_list(EXTENDED_STREAMING_HISTORY_FILEPATH)
print("-"*50)
audio_features.extract_audio_features()
print("-"*50)
audio_features.to_csv(AUDIO_FEATURES_FILEPATH)
print("-"*50)
audio_features.preview_dataframe()
print("-"*50, "\n", "-"*50, "\n", "-"*50)

full_dataset = FullDataset(EXTENDED_STREAMING_HISTORY_FILEPATH, AUDIO_FEATURES_FILEPATH)
print("-"*50)
full_dataset.create_full_dataset()
print("-"*50)
full_dataset.to_csv(FULL_DATASET_FILEPATH)
print("-"*50)
full_dataset.preview_dataframe()