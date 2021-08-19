import sys
import tweepy
import tweepy as tw
from pymongo import MongoClient
from tweepy import StreamListener

auth = tw.OAuthHandler("0C1hB0aVqax6OYRBl4P75TdPI", "keANh5Q6K6F10Ng9MWeSlNrk4NPiXXaFP2tkM8iGFoZYrQtWcJ")
auth.set_access_token("1358635825265741824-DQxcJCheNpfB4JhTsg3tI8ESL5JTFL",
                      "a8NJg5pRq7hUbZbuFIvlpemgVFUqzflwdEGuEH1VQUaCO")
api = tw.API(auth)
WORDS = ["covid", "emergency", "immune", "vaccine", "flu", "snow"]


class StreamListener(tweepy.StreamListener):
    count = 0
    fileName = 1;
    client = MongoClient("mongodb+srv://root:juhil7734@cluster0.hnmxh.mongodb.net/test")
    db = client["myMongoTweet"]
    collection = db["Tweet_stream - 1"]

    def on_status(self, status):
        StreamListener.count += 1
        description = status.user.description
        loc = status.user.location
        text = status.text
        coords = status.coordinates
        name = status.user.screen_name
        followers = status.user.followers_count
        created = status.created_at
        retweets = status.retweet_count

        StreamListener.collection.insert(dict(
            text=text,
            retweet_count=retweets,
            user_description=description,
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

        if StreamListener.count % 100 == 0:
            StreamListener.fileName += 1
            file = "Tweet_stream - " + str(StreamListener.fileName)
            StreamListener.collection = StreamListener.db[str(file)]

        if StreamListener.count == 4000:
            sys.exit()

    def on_error(self, status_code):
        if status_code == 420:
            return False


stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=WORDS)
