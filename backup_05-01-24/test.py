import pandas as pd # Read the CSV file
csv_file_path = 'output_file.csv'
existing_tweet_ids_df = pd.read_csv(csv_file_path)

# Extract 'tweet_id' values into a set for faster lookup
existing_tweet_ids_set = set(existing_tweet_ids_df['reply_id'].tolist())
print(existing_tweet_ids_set)
