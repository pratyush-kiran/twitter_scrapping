import requests
import AnalysiseSingleCommentSentiment
import pandas as pd
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

def insert_data_into_postgres(df):
    # # ############### Writing Data to Azure PostgreSQL #######################


    # Define the database connection URL
    # golak_mahapatra_odisha
    db_url = "postgresql://devuser:Sentimentanalysis%40123@db-campaign-analytics.postgres.database.azure.com:5432/demo_database?sslmode=require&sslrootcert=DigiCertGlobalRootCA.crt.pem"

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # DELETE data for the current date from the table
    delete_curdate_data = text("DELETE FROM twitter_hashtags WHERE date(file_created_date) = current_date")


    with engine.connect() as connection:
        connection.execute(delete_curdate_data)

        # Assuming 'df' is your DataFrame
        for index, row in df.iterrows():
            query = text("""

                INSERT INTO twitter_hashtags (
                    id, tweet_id, tweet_text, tweet_created_at, sentiment,
                    positive_score, neutral_score, negative_score, file_created_date, sentiment_desc, score
                ) VALUES (
                    :id, :tweet_id, :tweet_text, :tweet_created_at, :sentiment,
                    :positive_score, :neutral_score, :negative_score, :file_created_date, 
                    :sentiment_desc, :score
                );
                         
            """)
            values = {
                'id': row['unique_id'],
                'tweet_id': row['tweet_id'], 
                'tweet_text': row['tweet_text'], 
                'tweet_created_at': row['tweet_created_at'],
                'sentiment': row['sentiment'],       
                'positive_score': row['positive_score'], 
                'neutral_score': row['neutral_score'], 
                'negative_score': row['negative_score'],
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

# def search_tweets_by_hashtag(bearer_token, hashtag, count, days):
#     # Search tweets by hashtag with full text included
#     params = "tweet.fields=text,id,created_at"
#     url = f"https://api.twitter.com/2/tweets/search/recent?query=%23{hashtag}&{params}&max_results={count}"
#     headers = {
#         "Authorization": f"Bearer {bearer_token}",
#     }
#     response = requests.get(url, headers=headers)
#     if 'data' in response.json():
#         tweets = response.json().get("data")
#     else:
#         print("No Records found for this #HASHTAG")
#         tweets = []

#     return tweets

def search_tweets_by_hashtag(bearer_token, hashtag, count, days):

    days_ago = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = f"tweet.fields=text,id,created_at&start_time={days_ago}"
    url = f"https://api.twitter.com/2/tweets/search/recent?query=%23{hashtag}&{params}&max_results={count}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
    }
    response = requests.get(url, headers=headers)
    if 'data' in response.json():
        tweets = response.json().get("data")
    else:
        print("No Records found for this #HASHTAG")
        tweets = []

    return tweets


def main():
    hashtag = "BI_Prof_Bbsr"
    # hashtag = "india"
    count = 20
    days = 7 # only a maximum of 7 days can be retrieved
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAACxfrgEAAAAAy0a%2FEp%2Fc5Y21NycWWC1%2FENMd6is%3DhsICamrd4pjy00xIvGAUrFmGhLxH2CXXlwTMY7U5q7icNWFH8r'

    data = []
    tweets = search_tweets_by_hashtag(bearer_token, hashtag, count, days)
    for tweet in tweets:
        print(tweet)
        # print(tweet['id'])
        # print(tweet[])
        tweet_text = tweet['text']

        # get sentiment for the comments
        comment_sentiment=AnalysiseSingleCommentSentiment.analyze_sentiment(tweet_text) # Calling Fun to Analysise Semntiment
        single_sentiment=comment_sentiment["Overall_Sentiment"]
        single_positive_score=comment_sentiment["Positive_Score"]
        single_neutral_score=comment_sentiment["Neutral_Score"]
        single_negative_score=comment_sentiment["Negative_Score"]

        data.append({
            "tweet_id": tweet['id'],
            "tweet_text": tweet_text,
            "tweet_created_at": tweet['created_at'],
            "sentiment": single_sentiment,
            "positive_score": single_positive_score,
            "neutral_score": single_neutral_score,
            "negative_score": single_negative_score,
            "file_created_date": File_Created_Date
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

    # Getting Max Score
    df['score'] = df.apply(lambda row: max(row['positive_score'], row['negative_score'], row['neutral_score']), axis=1)*10

    df['sentiment_desc'] = df.apply(get_sentiment_desc, axis=1)

    print(df)
    #insert unto postgres
    insert_data_into_postgres(df)


    # ############### Storing into a CSV file locally #######################

    # Save DataFrame to CSV
    df.to_csv('twitter_hastags.csv', index=False)
    print("DataFrame stored in local storage succesfully !")

       
if __name__ == "__main__":
    main()
