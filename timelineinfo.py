import requests
import os
import json

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
# bearer_token = os.environ.get("BEARER_TOKEN")

bearer_token = 'AAAAAAAAAAAAAAAAAAAAACxfrgEAAAAAy0a%2FEp%2Fc5Y21NycWWC1%2FENMd6is%3DhsICamrd4pjy00xIvGAUrFmGhLxH2CXXlwTMY7U5q7icNWFH8r'


def create_url(user_id):
    # Replace with user ID below
    user_id=user_id
    # user_id = 1738210505867882496
    return "https://api.twitter.com/2/users/{}/tweets".format(user_id)


def get_params():
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    return {"tweet.fields": "created_at,public_metrics,conversation_id"}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()



def timeline_overall_info(user_id):
    url = create_url(user_id)
    params = get_params()
    json_response = connect_to_endpoint(url, params)
    # print(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response


# def main():
#     url = create_url()
#     params = get_params()
#     json_response = connect_to_endpoint(url, params)
#     print(json.dumps(json_response, indent=4, sort_keys=True))


# if __name__ == "__main__":
#     main()