from import_mongo import importDataToMongo, writeToMongo
from math import sqrt
from itertools import combinations
import random
from pyspark import SparkContext
from import_mongo import readFromMongo
from utils import getOrCreateSparkSession
from pyspark.sql import SparkSession
# import findspark


def parseVectorOnUser(line):
    return line[0], (line[1], float(line[2]))


# In[8]:


def parseVectorOnItem(line):
    return line[1], (line[0], float(line[2]))


# ***Get sample of n interactions of each movie***

# In[9]:


def sampleInteractions(item_id, user_with_rating, n):
    if len(user_with_rating) > n:
        return item_id, random.sample(user_with_rating, n)
    else:
        return item_id, user_with_rating


# In[10]:


def findUserPairs(item_id, users_with_rating):
    for user1, user2 in combinations(users_with_rating, 2):
        return (user1[0], user2[0]), (user1[1], user2[1])


# In[11]:


def cosine(dot_product, rating_norm_squared, rating2_norm_squared):
    numerator = dot_product
    denominator = rating_norm_squared * rating2_norm_squared
    return (numerator/(float(denominator))) if denominator else 0.0


# In[12]:


def calSim(user_pair, rating_pairs):
    sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
    for pair in rating_pairs:
        sum_xx += pow(pair[0], 2)
        sum_yy += pow(pair[1], 2)
        sum_xy += float(pair[1]) * float(pair[0])
        n += 1
    cos_sim = cosine(sum_xy, sqrt(sum_xx), sqrt(sum_yy))
    return user_pair, (cos_sim, n)


# In[13]:


def keyOnFirstUser(user_pair, item_sim_data):
    (user1_id, user2_id) = user_pair
    return user1_id, (user2_id, item_sim_data)


# In[14]:


def nearestNeighbors(user, users_and_sims, n):
    users_and_sims.sort(key=lambda x: x[1][0], reverse=True)
    return user, users_and_sims[:n]


# In[42]:


def topNRecommendations(user_id, user_sims, users_with_rating, n):
    from collections import defaultdict
    totals = defaultdict(int)
    sim_sums = defaultdict(int)

    for (neighbor, (sim, count)) in user_sims:
        unscored_items = users_with_rating.get(neighbor, None)

        if unscored_items:
            for (item, rating) in unscored_items:
                if neighbor != item:
                    totals[neighbor] += sim*rating
                    sim_sums[neighbor] += sim
    scored_items = [(total/sim_sums[item], item)
                    for item, total in totals.items()]

    scored_items.sort(reverse=True)

    ranked_items = [x[1] for x in scored_items]

    return user_id, ranked_items[:n]


# **convert df to rdd**

# In[16]:
class CF:
    spark = None

    def __init__(self, spark, ratings):
        CF.spark = spark
        self.ratings = ratings

    def processRecommendations(self):
        data = self.ratings

        rdd = data.rdd.map(list)
        item_user_pairs = rdd.map(parseVectorOnItem).groupByKey().map(
            lambda p: sampleInteractions(p[0], p[1], 500)).cache()
        # `(user1_id,user2_id) -> [(rating1_movieId1,rating2_movieId1),...]`

        pairwise_users = item_user_pairs.filter(lambda p: len(p[1]) > 1).map(
            lambda p: findUserPairs(p[0], p[1])).groupByKey()

        user_sims = pairwise_users.map(lambda p: calSim(p[0], p[1]))

        # ***group every similarity by user key***

        key_firstUser = user_sims.map(
            lambda p: keyOnFirstUser(p[0], p[1])).groupByKey()

        user_sims = key_firstUser.map(
            lambda p: nearestNeighbors(p[0], list(p[1]), 100))

        user_item_history = rdd.map(parseVectorOnUser).groupByKey().collect()

        ui_dict = {}

        for (user, items) in user_item_history:
            ui_dict[user] = items

        sc = CF.spark.sparkContext
        uib = sc.broadcast(ui_dict)

        user_item_recs = user_sims.map(
            lambda p: topNRecommendations(p[0], p[1], uib.value, 500)).collect()

        self.user_item_recs = user_item_recs
        return self.user_item_recs

    def getRecommendationList(self):
        self.processRecommendations()
        return self.user_item_recs

    def setRatings(self, ratings):
        self.ratings = ratings


# if __name__ == "__main__":
#     spark = getOrCreateSparkSession("mongo")
#     df = readFromMongo("ratings_copy", spark)
#     df = df.drop("_id")
#     df = df.drop("timestamp")
#     df = df[['userId', 'movieId', 'rating']]
#     df.withColumn("movieId", df.movieId.cast('string'))
#     cf = CF(spark, df)
#     result = cf.processRecommendations()
#     # print(result)
#     writeToMongo(spark, result, "recommendation")
