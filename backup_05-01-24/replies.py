import requests
import os
import json

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

def get_replies(conversation_id):
    # url for getting replies
    url = f"https://api.twitter.com/2/tweets/search/recent"

    params = {
        "query": f"conversation_id:{conversation_id}",
        "tweet.fields": "id,text,author_id,created_at",
    }

    response = connect_to_endpoint(url, params)
    # print(conversation_id)
    if "meta" in response and "result_count" in response["meta"] and response["meta"]["result_count"] > 0:
        # If there are results, print and return the data
        # print(json.dumps(response, indent=4, sort_keys=True))
        return response["data"]
    else:   
        # If there are no results, print a message and return None or an empty list, depending on your preference
        print("No results found for the given conversation ID.")
        return None  # or return [] or any other appropriate value

def comments_on_the_post(conversation_id):

    replies = get_replies(conversation_id)

    # print("Replies:")
    
    # for reply in replies:
    #     user_details = get_user_details(reply['author_id'])
    #     print(f"  Reply ID: {reply['id']}, Author ID: {reply['author_id']}, Comment Time: {reply['created_at']}, Username: {user_details['username']}, Gender: {user_details.get('gender', 'N/A')}, Name: {user_details.get('name', 'N/A')}, Location: {user_details.get('location', 'N/A')}, Text: {reply['text']}")

    return replies

# comments_on_the_post(1666802642436382720)
