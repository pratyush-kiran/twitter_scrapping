import requests
import json
import pandas as pd
import AnalysiseSingleCommentSentiment
from datetime import datetime, timezone, timedelta
import pytz
import uuid
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from io import BytesIO
from azure.storage.blob import BlobServiceClient

ist_timezone = pytz.timezone('Asia/Kolkata')
current_date_time = datetime.now(tz=ist_timezone)
# current_date_time = current_date_time - timedelta(days=1)
FileNameVariable = current_date_time.strftime("%Y%m%d%H%M%S")
File_Created_Date = current_date_time.strftime("%Y-%m-%d %H:%M:%S")

bearer_token = 'AAAAAAAAAAAAAAAAAAAAACxfrgEAAAAAy0a%2FEp%2Fc5Y21NycWWC1%2FENMd6is%3DhsICamrd4pjy00xIvGAUrFmGhLxH2CXXlwTMY7U5q7icNWFH8r'
# user_id = 1738210505867882496 #BI_PROF_BBSR
user_id = 861455487966912512 #PRATHAP KESHARI DEV

def get_sentiment_desc(row1):
    if  row1['score']>=8 and row1['sentiment']=='neutral':
        return 'Neutral'
    elif row1['sentiment']=='neutral' and row1['positive_score']>row1['negative_score']:
        return 'Positively Neutral'
    elif row1['sentiment']=='neutral' and row1['positive_score']<row1['negative_score']:
        return 'Negatively Neutral' 
    elif row1['score'] <= 6 and row1['sentiment']=='positive':
        return 'Moderately Positive'
    elif row1['score'] <= 6 and row1['sentiment']=='negative':
        return 'Moderately Negative'
    elif row1['score'] <= 8 and row1['sentiment']=='positive':
        return 'Positive'
    elif row1['score'] <= 8 and row1['sentiment']=='negative':
        return 'Negative'
    elif row1['score'] >= 8 and row1['sentiment']=='positive':
        return 'Extremely Positive'
    elif row1['score'] >= 8 and row1['sentiment']=='negative':
        return 'Extremely Negative'
    else:
        return row1['sentiment']
    
def insert_data_into_postgres():
    # # ############### Writing Data to Azure PostgreSQL #######################
    # Define the database connection URL
    # golak_mahapatra_odisha
    db_url = "postgresql://devuser:Sentimentanalysis%40123@db-campaign-analytics.postgres.database.azure.com:5432/demo_database?sslmode=require&sslrootcert=DigiCertGlobalRootCA.crt.pem"

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # DELETE data for the current date from the table
    delete_curdate_data = text("DELETE FROM twitter_mentions WHERE date(file_created_date) = current_date")

    with engine.connect() as connection:
        connection.execute(delete_curdate_data)

        for index, row in df.iterrows():
            query = text("""

                INSERT INTO twitter_mentions ( 
                        
                id, author_id, name, username, location, conversation_id, tweet_id, tweet_text, reply_to_tweet_id, tweet_created_at, sentiment, positive_score, neutral_score, negative_score, file_created_date, score, sentiment_desc

                ) VALUES (
                    :id, :author_id, :name, :username, :location, :conversation_id, :tweet_id, :tweet_text, :reply_to_tweet_id, :tweet_created_at, :sentiment, :positive_score, :neutral_score, :negative_score, :file_created_date, :score, :sentiment_desc
                );
            """)
            values = {
                'id': row['unique_id'],
                'author_id': row['author_id'], 
                'name': row['name'], 
                'username': row['username'], 
                'location': row['location'], 
                'conversation_id': row['conversation_id'],
                'tweet_id': row['tweet_id'], 
                'tweet_text': row['tweet_text'], 
                'reply_to_tweet_id': row['reply_to_tweet_id'], 
                'tweet_created_at': row['tweet_created_at'], 
                'sentiment': row['sentiment'],       
                'positive_score': row['positive_score'], 
                'neutral_score': row['neutral_score'], 
                'negative_score': row['negative_score'], 
                'file_created_date': row['file_created_date'],
                'score': row['score'],
                'sentiment_desc': row['sentiment_desc'],
            }        
            connection.execute(query, values)

        # Commit the changes to the database
        connection.commit()
        print("DataFrame wriiten to PostgreSQL successfully !")

    # Dispose of the database engine
    engine.dispose()

def insert_data_into_csv():
    df.to_csv('mentions.csv', index=False)
    print("DataFrame stored in local storage succesfully !")

def get_user_details(author_id):
    user_url = f"https://api.twitter.com/2/users/{author_id}"
    user_params = {"user.fields": "created_at,location,name,username"}

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "v2UserTweetsPython",
    }

    response = requests.get(user_url, headers=headers, params=user_params)

    if response.status_code == 200:
        user_data = response.json().get('data', {})
        # print(json.dumps(user_data, indent=4, sort_keys=True))
        return user_data
    else:
        raise Exception(f"User request returned an error: {response.status_code} {response.text}")


# def mentioned_timeline_info(user_id):
base_url = f"https://api.twitter.com/2/users/{user_id}/mentions"
params = {"tweet.fields": "id,author_id,text,created_at,conversation_id,in_reply_to_user_id"}

headers = {
    "Authorization": f"Bearer {bearer_token}",
    "User-Agent": "v2UserTweetsPython",
}

all_data = []

while True:
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        json_response = response.json()
        data = json_response.get('data', [])
        # print(json.dumps(data, indent=4, sort_keys=True))
        all_data.append(data)

        # Check for pagination
        meta = json_response.get('meta', {})
        next_token = meta.get('next_token')

        if next_token:
            params['pagination_token'] = next_token
        else:
            break

    else:
        raise Exception(f"Request returned an error: {response.status_code} {response.text}")
# print(all_data)
    
combined_data = []

for data_list in all_data:
    for tweet in data_list:
        print(tweet)
        author_id = tweet.get('author_id', '')
        user_details = get_user_details(author_id)
        tweet_created_at = tweet.get('created_at', '')


        comment_sentiment=AnalysiseSingleCommentSentiment.analyze_sentiment(tweet.get('text', '')) # Calling Fun to Analysise Semntiment
        single_sentiment=comment_sentiment["Overall_Sentiment"]
        single_positive_score=comment_sentiment["Positive_Score"]
        single_neutral_score=comment_sentiment["Neutral_Score"]
        single_negative_score=comment_sentiment["Negative_Score"]

        current_data = {
            "author_id": author_id,
            "name": user_details.get('name', 'N/A'),
            "username": user_details.get('username', 'N/A'),
            "location": user_details.get('location', 'N/A'),
            "conversation_id": tweet.get('conversation_id', 'N/A'),
            "tweet_id": tweet.get('id', 'N/A'),
            "tweet_text": tweet.get('text', 'N/A'),
            "reply_to_tweet_id": tweet.get('in_reply_to_user_id', 'N/A'),
            "tweet_created_at": tweet_created_at,
            "sentiment": single_sentiment,
            "positive_score": single_positive_score,
            "neutral_score": single_neutral_score,
            "negative_score": single_negative_score,
            "file_created_date": File_Created_Date,
        }
        combined_data.append(current_data)


    # print(json.dumps(combined_data, indent=4, sort_keys=True))

    # return combined_data

# list to DataFrame 
df = pd.DataFrame(combined_data)

# print(df)

# # Read the CSV file
# csv_file_path = 'output_file.csv'
# existing_tweet_ids_df = pd.read_csv(csv_file_path)

# # Extract 'tweet_id' values into a set for faster lookup
# existing_tweet_ids_set = set(existing_tweet_ids_df['reply_to_tweet_id'].tolist())
# print(existing_tweet_ids_set)

# # Filter the DataFrame based on 'tweet_id' not in the set
# df = df[~df['tweet_id'].isin(existing_tweet_ids_set)]



# Generate unique IDs using uuid and add them as a new column 'unique_id'
df['unique_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

# Set 'unique_id' as the first column
df = df[['unique_id'] + [col for col in df if col != 'unique_id']]

# Convert 'tweet_created_at' to a datetime object
df['tweet_created_at'] = pd.to_datetime(df['tweet_created_at'], format='%Y-%m-%dT%H:%M:%S.%fZ')

# Convert 'tweet_created_at' to IST
df['tweet_created_at'] = df['tweet_created_at'].dt.tz_localize(timezone.utc).dt.tz_convert(ist_timezone)

# Format the 'tweet_created_at' column in the desired format
df['tweet_created_at'] = df['tweet_created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Getting Max Score
df['score'] = df.apply(lambda row: max(row['positive_score'], row['negative_score'], row['neutral_score']), axis=1)*10

df['sentiment_desc'] = df.apply(get_sentiment_desc, axis=1)


# insert data into postgres
insert_data_into_postgres()
insert_data_into_csv()



