import requests
import os
import json

bearer_token = 'AAAAAAAAAAAAAAAAAAAAACxfrgEAAAAAy0a%2FEp%2Fc5Y21NycWWC1%2FENMd6is%3DhsICamrd4pjy00xIvGAUrFmGhLxH2CXXlwTMY7U5q7icNWFH8r'

def create_url(usernames):
    # Specify the usernames that you want to lookup below
    # You can enter up to 100 comma-separated values.
    # usernames = "usernames=BI_Prof_Bbsr"
    usernames=usernames
    user_fields = "user.fields=description,created_at,public_metrics,location,name"
    # User fields are adjustable, options include:
    # created_at, description, entities, id, location, name,
    # pinned_tweet_id, profile_image_url, protected,
    # public_metrics, url, username, verified, and withheld
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    return url

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r

def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth,)
    # print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def acclunt_level_info(usernames):
    url = create_url(usernames)
    json_response = connect_to_endpoint(url)
    # print(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response

