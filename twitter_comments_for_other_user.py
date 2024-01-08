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


# Function to get account level information
def get_account_level_info(username):
    acc_lvl_result = userinfo.acclunt_level_info(f"usernames={username}")
    return acc_lvl_result['data'][0]

# Function to get timeline information
def get_timeline_info(user_id):
    timeline_data = timelineinfo.timeline_overall_info(user_id)
    return timeline_data['data']

# # Example username
# username = "BI_Prof_Bbsr"
# user_id = 1738210505867882496

username = 'KeshariDeb'
user_id = '861455487966912512'


# Get account level information
account_info = get_account_level_info(username)

# Get timeline information
timeline_info = get_timeline_info(user_id)


# Function to get replies for a conversation ID
def get_replies(conversation_id):
    return replies.comments_on_the_post(conversation_id)



# Get account level information
account_info = get_account_level_info(username)

# Get timeline information
timeline_info = get_timeline_info(user_id)


# Create lists to store data
data = []
# conversation_id_list = []


# Loop through tweets in the timeline
for tweet in timeline_info:

    print("--->", "Tweet Text :",tweet['text'], "\n", "Conversation ID :", tweet['conversation_id'], "\n")
    

    conversation_id = tweet['conversation_id']
    # conversation_id_list.append(conversation_id)


    # Get replies for the conversation ID
    replies_data = get_replies(conversation_id)

    for reply in replies_data:
        mention = f"@{username}"
        cleaned_comment = reply['text'].replace(mention, '').strip()
        # print(cleaned_comment)
        user_details = replies.get_user_details(reply['author_id'])
        
        print("  --->", "reply text:", cleaned_comment)
        print("  --->", "username:", user_details['username'])
        print("  --->", "reply id:", reply['id'])
        print("  --->", "author id:", reply['author_id'])
        print("  --->", "gender:", user_details.get('gender', 'N/A'))
        print("  --->", "location", user_details.get('location', 'N/A'))
        print("  --->", "reply_created_at:", reply['created_at'])
        print("*" * 50)










        
    #     # Append data to the list
    #     data.append({
    #         "user_id": account_info['id'],
    #         "username": account_info['username'],
    #         "followers_count": account_info['public_metrics']['followers_count'],
    #         "following_count": account_info['public_metrics']['following_count'],
    #         "tweet_id": tweet['id'],
    #         "tweet_text": tweet['text'],
    #         "tweet_created_at": tweet['created_at'],
    #         "conversation_id": conversation_id,
    #         "reply_id": reply['id'],
    #         "author_id": reply['author_id'],
    #         "author_username": user_details['username'],
    #         "author_gender": user_details.get('gender', 'N/A'),
    #         "author_location": user_details.get('location', 'N/A'),
    #         "reply_text": cleaned_comment,
    #         "reply_created_at": reply['created_at'],
    #         "sentiment": single_sentiment,
    #         "positive_score": single_positive_score,
    #         "neutral_score": single_neutral_score,
    #         "negative_score": single_negative_score,
    #         "retweet_count": tweet['public_metrics']['retweet_count'],
    #         "like_count": tweet['public_metrics']['like_count'],
    #         "reply_count": tweet['public_metrics']['reply_count'],
    #         "quote_count": tweet['public_metrics']['quote_count'],
    #         "bookmark_count": tweet['public_metrics']['bookmark_count'],
    #         "impression_count": tweet['public_metrics']['impression_count'],
    #         "file_created_date": File_Created_Date,
    #     })

# print(conversation_id_list)