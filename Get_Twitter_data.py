import userinfo
import timelineinfo
import replies
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
FileNameVariable = current_date_time.strftime("%Y%m%d%H%M%S")
File_Created_Date=current_date_time.strftime("%Y-%m-%d %H:%M:%S")

# Your Azure Storage account details
account_name = "campaignanalyticsstorage"
account_key = "LBsv1gd3OK0swNIuNbpew5fonwkHN0BN5b0FZev1AEWRTg6GDigzT1N2fe7YCjrWhvMBc2uufxmi+AStlP0H7w=="

container_name = "golakmohapatratwitter"
blob_name = f"twitteroutput{FileNameVariable}.csv"

# Credentials
api_key = 'AIzaSyDTsel33TrYqdNRFx2azkkV8d5p9xcz62Q'
channel_id='UCWLZ4Q_MeqFJqZGfV4q0JKg'


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
    delete_curdate_data = text("DELETE FROM twitter_messages WHERE date(file_created_date) = current_date")


    with engine.connect() as connection:
        connection.execute(delete_curdate_data)

        # Assuming 'df' is your DataFrame
        for index, row in df.iterrows():
            query = text("""

                INSERT INTO twitter_messages (
                    id, user_id, username, followers_count, following_count,
                    tweet_id, tweet_text, tweet_created_at, conversation_id,
                    reply_id, author_id, author_username, author_gender,
                    author_location, reply_text, reply_created_at, sentiment,
                    positive_score, neutral_score, negative_score, retweet_count,
                    like_count, reply_count, quote_count, bookmark_count,
                    impression_count, file_created_date, sentiment_desc, score
                ) VALUES (
                    :id, :user_id, :username, :followers_count, :following_count,
                    :tweet_id, :tweet_text, :tweet_created_at, :conversation_id,
                    :reply_id, :author_id, :author_username, :author_gender,
                    :author_location, :reply_text, :reply_created_at, :sentiment,
                    :positive_score, :neutral_score, :negative_score, :retweet_count,
                    :like_count, :reply_count, :quote_count, :bookmark_count,
                    :impression_count, :file_created_date, :sentiment_desc, :score
                );
            """)
            values = {
                'id': row['unique_id'],
                'user_id': row['user_id'], 
                'username': row['username'], 
                'followers_count': row['followers_count'], 
                'following_count': row['following_count'],
                'tweet_id': row['tweet_id'], 
                'tweet_text': row['tweet_text'], 
                'tweet_created_at': row['tweet_created_at'], 
                'conversation_id': row['conversation_id'],
                'reply_id': row['reply_id'], 
                'author_id': row['author_id'], 
                'author_username': row['author_username'], 
                'author_gender': row['author_gender'],
                'author_location': row['author_location'], 
                'reply_text': row['reply_text'], 
                'reply_created_at': row['reply_created_at'], 
                'sentiment': row['sentiment'],       
                'positive_score': row['positive_score'], 
                'neutral_score': row['neutral_score'], 
                'negative_score': row['negative_score'], 
                'retweet_count': row['retweet_count'],        
                'like_count': row['like_count'], 
                'reply_count': row['reply_count'], 
                'quote_count': row['quote_count'], 
                'bookmark_count': row['bookmark_count'],        
                'impression_count': row['impression_count'], 
                'file_created_date': row['file_created_date'], 
                'sentiment_desc': row['sentiment_desc'],
                'score': row['score']    
            }        
            
            connection.execute(query, values)

        # Commit the changes to the database
        connection.commit()
        print("DataFrame wriiten to PostgreSQL successfully !")

    # Dispose of the database engine
    engine.dispose()

def migrate_to_azure_blob():
    # ############### Migrate to Azure BLOB #######################

    # Convert DataFrame to CSV string in memory
    csv_data = df.to_csv(index=False)
    csv_bytes = csv_data.encode('utf-8')
    csv_stream = BytesIO(csv_bytes)

    # Create a connection string for the Azure Storage account
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    # Create a BlobServiceClient 
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Upload the CSV data from the in-memory stream to the blob
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(csv_stream, overwrite=True)
    print(f"DataFrame written to Azure Blob Storage: {blob_name}")


# Function to get account level information
def get_account_level_info(username):
    acc_lvl_result = userinfo.acclunt_level_info(f"usernames={username}")
    return acc_lvl_result['data'][0]

# Function to get timeline information
def get_timeline_info(user_id):
    timeline_data = timelineinfo.timeline_overall_info(user_id)
    return timeline_data['data']

# Function to get replies for a conversation ID
def get_replies(conversation_id):
    return replies.comments_on_the_post(conversation_id)

# # Example username
username = "BI_Prof_Bbsr"
user_id = 1738210505867882496

# username = 'KeshariDeb'
# user_id = '861455487966912512'

# Get account level information
account_info = get_account_level_info(username)

# Get timeline information
timeline_info = get_timeline_info(user_id)


# Create lists to store data
data = []

# Loop through tweets in the timeline
for tweet in timeline_info:

    print(tweet)
    # Get conversation ID
    conversation_id = tweet['conversation_id']

    # Get replies for the conversation ID
    replies_data = get_replies(conversation_id)

    # Loop through replies
    for reply in replies_data:
        mention = f"@{username}"
        cleaned_comment = reply['text'].replace(mention, '').strip()
        user_details = replies.get_user_details(reply['author_id'])
        
        comment_sentiment=AnalysiseSingleCommentSentiment.analyze_sentiment(cleaned_comment) # Calling Fun to Analysise Semntiment
        single_sentiment=comment_sentiment["Overall_Sentiment"]
        single_positive_score=comment_sentiment["Positive_Score"]
        single_neutral_score=comment_sentiment["Neutral_Score"]
        single_negative_score=comment_sentiment["Negative_Score"]

        
        # Append data to the list
        data.append({
            "user_id": account_info['id'],
            "username": account_info['username'],
            "followers_count": account_info['public_metrics']['followers_count'],
            "following_count": account_info['public_metrics']['following_count'],
            "tweet_id": tweet['id'],
            "tweet_text": tweet['text'],
            "tweet_created_at": tweet['created_at'],
            "conversation_id": conversation_id,
            "reply_id": reply['id'],
            "author_id": reply['author_id'],
            "author_username": user_details['username'],
            "author_gender": user_details.get('gender', 'N/A'),
            "author_location": user_details.get('location', 'N/A'),
            "reply_text": cleaned_comment,
            "reply_created_at": reply['created_at'],
            "sentiment": single_sentiment,
            "positive_score": single_positive_score,
            "neutral_score": single_neutral_score,
            "negative_score": single_negative_score,
            "retweet_count": tweet['public_metrics']['retweet_count'],
            "like_count": tweet['public_metrics']['like_count'],
            "reply_count": tweet['public_metrics']['reply_count'],
            "quote_count": tweet['public_metrics']['quote_count'],
            "bookmark_count": tweet['public_metrics']['bookmark_count'],
            "impression_count": tweet['public_metrics']['impression_count'],
            "file_created_date": File_Created_Date,
        })


# Create a DataFrame from the collected data
df = pd.DataFrame(data)

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

# Convert 'Reply Created at' to a datetime object
df['reply_created_at'] = pd.to_datetime(df['reply_created_at'], format='%Y-%m-%dT%H:%M:%S.%fZ')

# Convert 'Reply Created at' to IST
df['reply_created_at'] = df['reply_created_at'].dt.tz_localize(timezone.utc).dt.tz_convert(ist_timezone)

# Format the 'Reply Created at' column in the desired format
df['reply_created_at'] = df['reply_created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Getting Max Score
df['score'] = df.apply(lambda row: max(row['positive_score'], row['negative_score'], row['neutral_score']), axis=1)*10

df['sentiment_desc'] = df.apply(get_sentiment_desc, axis=1)
       
#wow
a = 10

#insert unto postgres
insert_data_into_postgres()

# migrate_to_azure_blob()



# ############### Storing into a CSV file locally #######################

# Save DataFrame to CSV
df.to_csv('output_file.csv', index=False)
print("DataFrame stored in local storage succesfully !")

# Display the DataFrame
# print(df)
