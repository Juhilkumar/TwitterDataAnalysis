import sys

import tweepy as tw
from pymongo import MongoClient

auth = tw.OAuthHandler("0C1hB0aVqax6OYRBl4P75TdPI", "keANh5Q6K6F10Ng9MWeSlNrk4NPiXXaFP2tkM8iGFoZYrQtWcJ")
auth.set_access_token("1358635825265741824-DQxcJCheNpfB4JhTsg3tI8ESL5JTFL",
                      "a8NJg5pRq7hUbZbuFIvlpemgVFUqzflwdEGuEH1VQUaCO")
api = tw.API(auth)

client = MongoClient("mongodb+srv://root:juhil7734@cluster0.hnmxh.mongodb.net/test")
db = client["myMongoTweet"]
collection = db["Tweet_search - 1"]
OR = ' ' + 'OR' + ' '
WORDS = ['covid', 'emergency', 'immune', 'vaccine', 'flu', 'snow']
Search_Keywords = OR.join(WORDS)
date_since = "2020-01-17"

tweets = tw.Cursor(api.search,
                   q=Search_Keywords,
                   lang="en",
                   since=date_since).items(4000)

count = 0
fileName = 1;

for index, tweet in enumerate(tweets):
    count += 1
    loc = tweet.user.location
    text = tweet.text
    coords = tweet.coordinates
    name = tweet.user.screen_name
    followers = tweet.user.followers_count
    created = tweet.created_at
    retweets = tweet.retweet_count

    collection.insert(dict(
        text=text,
        retweet_count=retweets,
        user_location=loc,
        coordinates=coords,
        user_name=name,
        user_followers=followers,
        created=created, ))
    print("Tweet :" + str(text))
    print("UserName :" + str(name))
    print("Created :" + str(created))
    print("User_location :" + str(loc))
    print("User_followers :" + str(followers))
    print("Retweet_count :" + str(retweets))
    print("Coordinates :" + str(coords) + "\n")

    if count % 100 == 0:
        fileName += 1
        file = "Tweet_search - " + str(fileName)
        collection = db[str(file)]

    if count == 4000:
        sys.exit()
