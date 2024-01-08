import requests
import os
import pandas as pd
import json
from datetime import datetime, timedelta
import pytz

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAACxfrgEAAAAAy0a%2FEp%2Fc5Y21NycWWC1%2FENMd6is%3DhsICamrd4pjy00xIvGAUrFmGhLxH2CXXlwTMY7U5q7icNWFH8r'
# os.environ.get("BEARER_TOKEN")


def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    # print("Response Status Code:", response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(response.status_code, response.text)
        )
    return response.json()

def get_user_details(user_id):
    # url for getting user details
    url = f"https://api.twitter.com/2/users/{user_id}"

    params = {
        "user.fields": "id,username,created_at,description,public_metrics,location",
    }

    response = connect_to_endpoint(url, params)
    
    return response["data"]

def get_start_time():

    with open('last_fetch_datetime.txt') as f:
        start_time = f.readline()


    






    # df = pd.read_csv("last_fetch_datetime.csv")
    # start_time = df['Last Fetch DateTime'][0]
    # print(start_time)

    ist_timezone = pytz.timezone("Asia/Kolkata")

    ist_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    ist_datetime = ist_timezone.localize(ist_datetime)

    # Convert to UTC
    utc_datetime = ist_datetime.astimezone(pytz.utc)

    formatted_utc_str = utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # print(formatted_utc_str)
    return formatted_utc_str

def get_replies(conversation_id):
    # url for getting replies
    # start_time = (datetime.utcnow() - timedelta(days=90)).isoformat() + "Z"
    start_time = get_start_time()
    # print(start_time)
    url = f"https://api.twitter.com/2/tweets/search/recent"

    params = {
        "query": f"conversation_id:{conversation_id}",
        "tweet.fields": "id,text,author_id,created_at",
        "start_time": start_time,
    }

    response = connect_to_endpoint(url, params)
    # print(conversation_id)
    if "meta" in response and "result_count" in response["meta"] and response["meta"]["result_count"] > 0:
        # If there are results, print and return the data
        # print(json.dumps(response, indent=4, sort_keys=True))
        return response["data"]
    else:   
        # If there are no results, print a message and return None or an empty list, depending on your preference
        print("No new tweets found for this tweet_id.")
        return []  
    # return response

def comments_on_the_post(conversation_id):
    replies = get_replies(conversation_id)

    # print(replies)

    for reply in replies:
        # user_details = get_user_details(reply['author_id'])
        print(f"  Text: {reply['text']}, Comment Time: {reply['created_at']}, Reply ID: {reply['id']}, Author ID: {reply['author_id']}")
        print("-" * 50)

    print("*" * 100)

    return replies

# comments_on_the_post(1738213282694221936)
