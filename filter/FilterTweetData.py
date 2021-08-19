import re
from pymongo import MongoClient


def clean_data(txt):
    try:
        txt = " ".join(
            re.sub("([\U0001F1E0-\U0001F1FF]) | ([\U0001F300-\U0001F5FF]) |([\U0001F600-\U0001F64F]) |([\U0001F300-\U0001F5FF]) |([^a-zA-Z0-9]+)|(\w+:\/\/\S+)", " ",
                   txt).split())
    except:
        pass
    return txt


def main():
    client = MongoClient("mongodb+srv://root:juhil7734@cluster0.hnmxh.mongodb.net/test")
    db_unprocessed = client["myMongoTweet"]
    db_processed = client["cleaned"]
    processed_collection = db_processed["Tweet_cleaned - 1"]

    for i in range(1, 40):

        file = "Tweet_stream - " + str(i)
        collection = db_unprocessed[str(file)]
        tweetList = collection.find()

        fileWrite = "Tweet_cleaned - " + str(i)
        processed_collection = db_processed[str(fileWrite)]

        for tweet_row in tweetList:
            tweet_row["_id"] = clean_data(tweet_row["_id"])
            tweet_row["text"] = clean_data(tweet_row["text"])
            tweet_row["retweet_count"] = clean_data(tweet_row["retweet_count"])
            tweet_row["user_location"] = clean_data(tweet_row["user_location"])
            tweet_row["user_description"] = clean_data(tweet_row["user_description"])
            tweet_row["coordinates"] = clean_data(tweet_row["coordinates"])
            tweet_row["user_name"] = clean_data(tweet_row["user_name"])
            tweet_row["user_followers"] = clean_data(tweet_row["user_followers"])
            tweet_row["created"] = clean_data(tweet_row["created"])
            processed_collection.insert_one(tweet_row)
            print(tweet_row)

if __name__ == "__main__":
    main()
