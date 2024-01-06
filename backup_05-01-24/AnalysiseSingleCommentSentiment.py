from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient


def analyze_sentiment(input_string) -> None:
    # [START analyze_sentiment]
    

    endpoint = "https://azure-ai-campaign-analytics.cognitiveservices.azure.com/"  
    key = "0e7ed91c74e5469281834b3f2af958fb"

    text_analytics_client = TextAnalyticsClient(endpoint=endpoint, credential=AzureKeyCredential(key))

    documents =[input_string]
    
    #["Change is inevitable. Change will always happen, but you have to apply direction to change, and that's when it's progress.", 'Avg', 'not at all useful try something helpful for the society .. disliked', 'Not at all useful', 'Good', 'ðŸ˜', 'Good start', 'comment 3.', 'test comment', 'comment 1']

    result_dict={}
    result = text_analytics_client.analyze_sentiment(documents, show_opinion_mining=True)
    docs = [doc for doc in result if not doc.is_error]
    
    # print("Let's visualize the sentiment of each of these documents")
    # for idx, doc in enumerate(docs):
    #     print(f"Document text: {documents[idx]}")
    #     print(f"Overall sentiment: {doc.sentiment}")
    # [END analyze_sentiment]
        # Overall_Sentiment=doc.sentiment
    reviews = [doc for doc in docs ]
    for idx, review in enumerate(reviews):
        for sentence in review.sentences:
            # print(sentence)
            # print("...Sentence '{}' has sentiment '{}' with confidence scores '{}'".format(
            #     sentence.text,
            #     sentence.sentiment,
            #     sentence.confidence_scores
            # #     )
            # )
            result_dict={"Overall_Sentiment":sentence.sentiment,
                         "Positive_Score":sentence.confidence_scores['positive'],
                         "Neutral_Score":sentence.confidence_scores['neutral'],
                         "Negative_Score":sentence.confidence_scores['negative'],                       
                        }
        # positive=Overall_Sentiment.positive
        # print(positive)
    return result_dict



# if __name__ == '__main__':
# #     # list_inp=['Nice Video but can be improved', 'Ordinary people think merely of spending time, great people think of using it', "You've got to think about the big things while you're doing the small things so that all the small things go in the right direction.", 'wow, what a video', '<a href="about:invalid#zCSafez"></a>', "don't make such faltu video", 'the video is not useful at all... total time waste ..', 'Time wasting vdo', 'good job', 'ðŸ˜']
#     list_inp="U Guys Nailed It"
#     res=analyze_sentiment(list_inp)
#     print(res)
#     # print("Positive:",res['positive'])


